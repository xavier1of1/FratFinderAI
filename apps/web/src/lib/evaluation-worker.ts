import { randomUUID } from "crypto";
import os from "os";

import { runBenchmarkExecution } from "@/lib/benchmark-runner";
import { runCampaignExecution } from "@/lib/campaign-runner";
import {
  captureEvaluationWorkerSnapshot,
  claimNextEvaluationJob,
  completeEvaluationJob,
  failEvaluationJob,
  heartbeatEvaluationJobLease,
  type EvaluationJobRecord,
  releaseEvaluationJobLease,
  updateEvaluationJobPreconditions,
} from "@/lib/repositories/evaluation-job-repository";
import { resolveOpsAlertsByFingerprintPrefix, upsertOpenOpsAlert } from "@/lib/repositories/ops-alert-repository";
import {
  heartbeatRuntimeWorker,
  stopRuntimeWorker,
  upsertRuntimeWorker,
} from "@/lib/repositories/runtime-worker-repository";
import { failBenchmarkRun, getBenchmarkRun } from "@/lib/repositories/benchmark-repository";
import { getCampaignRun, updateCampaignRun } from "@/lib/repositories/campaign-run-repository";

const EVALUATION_WORKER_ID = `evaluation-worker:${os.hostname()}:${process.pid}`;
const EVALUATION_WORKER_LEASE_SECONDS = Math.max(30, Number(process.env.EVALUATION_WORKER_LEASE_SECONDS ?? 120));
const EVALUATION_HEARTBEAT_INTERVAL_MS = Math.max(
  10_000,
  Math.min(60_000, Math.floor(EVALUATION_WORKER_LEASE_SECONDS * 500))
);

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isStrictIsolationAvailable(preconditions: Record<string, unknown>): boolean {
  const activeWorkersByLane =
    preconditions.activeWorkersByLane && typeof preconditions.activeWorkersByLane === "object"
      ? (preconditions.activeWorkersByLane as Record<string, unknown>)
      : {};
  const blockingLanes = ["request", "campaign", "benchmark", "contact_resolution"];
  return blockingLanes.every((lane) => Number(activeWorkersByLane[lane] ?? 0) <= 0);
}

async function failRunForIsolation(job: EvaluationJobRecord, error: string): Promise<void> {
  if (job.benchmarkRunId) {
    const run = await getBenchmarkRun(job.benchmarkRunId);
    if (run) {
      await failBenchmarkRun({
        id: run.id,
        error,
        summary: run.summary,
        samples: run.samples,
      });
    }
  }

  if (job.campaignRunId) {
    const run = await getCampaignRun(job.campaignRunId);
    if (run) {
      await updateCampaignRun({
        id: run.id,
        status: "failed",
        finishedAtNow: true,
        lastError: error,
      });
    }
  }
}

function evaluationAlertFingerprint(job: EvaluationJobRecord): string {
  if (job.benchmarkRunId) {
    return `evaluation:benchmark:${job.benchmarkRunId}:`;
  }
  if (job.campaignRunId) {
    return `evaluation:campaign:${job.campaignRunId}:`;
  }
  return `evaluation:job:${job.id}:`;
}

async function executeEvaluationJob(job: EvaluationJobRecord, leaseToken: string): Promise<void> {
  let heartbeat: NodeJS.Timeout | null = null;
  try {
    const preconditions = await captureEvaluationWorkerSnapshot();
    const contaminationStatus = isStrictIsolationAvailable(preconditions) ? "isolated" : "shared_live";
    await updateEvaluationJobPreconditions({
      jobId: job.id,
      preconditions,
    });

    if (job.isolationMode === "strict_live_isolated" && contaminationStatus !== "isolated") {
      const error = "Strict live isolation is not currently available for this evaluation job.";
      await upsertOpenOpsAlert({
        alertScope: job.jobKind === "campaign_run" ? "campaign" : "benchmark",
        alertType: "strict_isolation_unavailable",
        severity: "warning",
        message: error,
        fingerprint: `${evaluationAlertFingerprint(job)}strict-isolation`,
        benchmarkRunId: job.benchmarkRunId,
        campaignRunId: job.campaignRunId,
        sourceSlug: job.sourceSlug,
        payload: { contaminationStatus, preconditions },
      });
      await failRunForIsolation(job, error);
      await failEvaluationJob({
        jobId: job.id,
        error,
        result: { contaminationStatus, preconditions },
      });
      return;
    }

    heartbeat = setInterval(() => {
      void heartbeatRuntimeWorker(EVALUATION_WORKER_ID, EVALUATION_WORKER_LEASE_SECONDS);
      void heartbeatEvaluationJobLease({
        jobId: job.id,
        workerId: EVALUATION_WORKER_ID,
        leaseToken,
        leaseSeconds: EVALUATION_WORKER_LEASE_SECONDS,
      });
    }, EVALUATION_HEARTBEAT_INTERVAL_MS);

    if (job.jobKind === "benchmark_run" && job.benchmarkRunId) {
      await runBenchmarkExecution(job.benchmarkRunId, {
        preconditions,
        isolationMode: job.isolationMode,
        contaminationStatus,
      });
    } else if (job.jobKind === "campaign_run" && job.campaignRunId) {
      await runCampaignExecution(job.campaignRunId);
    } else {
      throw new Error(`Unsupported evaluation job payload: ${job.id}`);
    }

    await completeEvaluationJob({
      jobId: job.id,
      result: {
        contaminationStatus,
        preconditions,
      },
    });
    await resolveOpsAlertsByFingerprintPrefix({
      prefix: evaluationAlertFingerprint(job),
      resolvedReason: "evaluation_job_succeeded",
      metadata: {
        jobKind: job.jobKind,
        benchmarkRunId: job.benchmarkRunId,
        campaignRunId: job.campaignRunId,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await upsertOpenOpsAlert({
      alertScope: job.jobKind === "campaign_run" ? "campaign" : "benchmark",
      alertType: "evaluation_failed",
      severity: "critical",
      message,
      fingerprint: `${evaluationAlertFingerprint(job)}failed`,
      benchmarkRunId: job.benchmarkRunId,
      campaignRunId: job.campaignRunId,
      sourceSlug: job.sourceSlug,
      payload: {
        jobId: job.id,
        jobKind: job.jobKind,
      },
    });
    await failEvaluationJob({
      jobId: job.id,
      error: message,
    });
    throw error;
  } finally {
    if (heartbeat) {
      clearInterval(heartbeat);
    }
    await releaseEvaluationJobLease({
      jobId: job.id,
      workerId: EVALUATION_WORKER_ID,
      leaseToken,
    });
  }
}

export async function runEvaluationWorker(options?: {
  once?: boolean;
  pollMs?: number;
  limit?: number;
}): Promise<{ processed: number }> {
  const pollMs = Math.max(1_000, Number(options?.pollMs ?? 5_000));
  const limit = Math.max(1, Number(options?.limit ?? 100));
  let processed = 0;

  await upsertRuntimeWorker({
    workerId: EVALUATION_WORKER_ID,
    workloadLane: "evaluation",
    runtimeOwner: "evaluation_worker",
    leaseSeconds: EVALUATION_WORKER_LEASE_SECONDS,
  });

  try {
    while (processed < limit) {
      await heartbeatRuntimeWorker(EVALUATION_WORKER_ID, EVALUATION_WORKER_LEASE_SECONDS);
      const leaseToken = randomUUID();
      const job = await claimNextEvaluationJob({
        workerId: EVALUATION_WORKER_ID,
        leaseToken,
        leaseSeconds: EVALUATION_WORKER_LEASE_SECONDS,
      });

      if (!job) {
        if (options?.once) {
          break;
        }
        await delay(pollMs);
        continue;
      }

      processed += 1;
      await executeEvaluationJob(job, leaseToken);

      if (options?.once) {
        break;
      }
    }
  } finally {
    await stopRuntimeWorker(EVALUATION_WORKER_ID);
  }

  return { processed };
}
