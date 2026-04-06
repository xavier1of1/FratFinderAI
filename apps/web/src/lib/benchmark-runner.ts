import { spawn } from "child_process";
import { randomUUID } from "crypto";
import { existsSync } from "fs";
import os from "os";
import path from "path";

import {
  completeBenchmarkRun,
  computeBenchmarkShadowDiffWindow,
  failBenchmarkRun,
  getBenchmarkRun,
  getFieldJobStatusSnapshot,
  markBenchmarkRunStarted,
  upsertBenchmarkShadowDiff
} from "@/lib/repositories/benchmark-repository";
import {
  claimBenchmarkRunLease,
  heartbeatBenchmarkRunLease,
  heartbeatRuntimeWorker,
  releaseBenchmarkRunLease,
  stopRuntimeWorker,
  upsertRuntimeWorker,
} from "@/lib/repositories/runtime-worker-repository";
import type {
  BenchmarkCycleSample,
  BenchmarkFieldName,
  BenchmarkIsolationMode,
  BenchmarkQueueSnapshot,
  BenchmarkRunConfig,
  BenchmarkRunSummary
} from "@/lib/types";

const activeRuns = new Set<string>();
const BENCHMARK_WORKER_ID = `benchmark-worker:${os.hostname()}:${process.pid}`;
const BENCHMARK_WORKER_LEASE_SECONDS = Math.max(30, Number(process.env.BENCHMARK_WORKER_LEASE_SECONDS ?? 90));
const BENCHMARK_HEARTBEAT_INTERVAL_MS = Math.max(10_000, Math.min(60_000, Math.floor(BENCHMARK_WORKER_LEASE_SECONDS * 500)));

interface ProcessFieldJobResult {
  processed: number;
  requeued: number;
  failedTerminal: number;
  queueTriage?: {
    invalidCancelled?: number;
    repairQueued?: number;
  };
  chapterRepair?: {
    promotedToCanonical?: number;
    reconciledHistorical?: number;
  };
}

interface BenchmarkTotals {
  processed: number;
  requeued: number;
  failedTerminal: number;
  invalidBlocked: number;
  repairableBlocked: number;
  repairPromoted: number;
  reconciledHistorical: number;
}

interface CrawlWarmupResult {
  runtimeMode: string;
  recordsSeen: number;
  recordsUpserted: number;
  pagesProcessed: number;
}

interface BenchmarkExecutionContext {
  preconditions?: Record<string, unknown>;
  isolationMode?: BenchmarkIsolationMode;
  contaminationStatus?: "isolated" | "shared_live";
}
const BENCHMARK_TIMEOUT_MIN_MS = 120_000;
const BENCHMARK_TIMEOUT_MAX_MS = 900_000;
const BENCHMARK_TIMEOUT_BASE_OVERHEAD_MS = 80_000;

function estimateBenchmarkCycleTimeoutMs(config: BenchmarkRunConfig): number {
  const override = Number(process.env.BENCHMARK_CYCLE_TIMEOUT_MS ?? "");
  if (Number.isFinite(override) && override > 0) {
    return Math.max(BENCHMARK_TIMEOUT_MIN_MS, Math.min(BENCHMARK_TIMEOUT_MAX_MS, Math.round(override)));
  }

  const perJobMsByField: Record<BenchmarkFieldName, number> = {
    find_website: 20_000,
    find_email: 10_000,
    find_instagram: 9_000,
    all: 14_000
  };

  const perJobMs = perJobMsByField[config.fieldName] ?? 10_000;
  const runtimeMultiplier =
    config.fieldJobRuntimeMode === "langgraph_shadow" || config.fieldJobRuntimeMode === "langgraph_primary"
      ? 1.8
      : 1.0;
  const requestedWorkers = Math.max(config.workers, 1);
  const effectiveWorkers = Math.max(1, Math.min(requestedWorkers, config.limitPerCycle));
  const predictedProcessingMs = ((config.limitPerCycle * perJobMs) / effectiveWorkers) * runtimeMultiplier;
  const predictedTotalMs = predictedProcessingMs + BENCHMARK_TIMEOUT_BASE_OVERHEAD_MS;

  return Math.max(BENCHMARK_TIMEOUT_MIN_MS, Math.min(BENCHMARK_TIMEOUT_MAX_MS, Math.round(predictedTotalMs)));
}

function isLanggraphRuntime(mode: string | undefined): boolean {
  return mode === "langgraph_shadow" || mode === "langgraph_primary";
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function findRepositoryRoot(): string {
  let currentDir = process.cwd();
  for (let index = 0; index < 6; index += 1) {
    if (existsSync(path.join(currentDir, "pnpm-workspace.yaml"))) {
      return currentDir;
    }
    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      break;
    }
    currentDir = parentDir;
  }
  return process.cwd();
}

function parseTrailingJson<T>(output: string): T {
  const trimmed = output.trim();
  for (let index = trimmed.lastIndexOf("{"); index >= 0; index = trimmed.lastIndexOf("{", index - 1)) {
    const candidate = trimmed.slice(index);
    try {
      return JSON.parse(candidate) as T;
    } catch {
      // continue scanning backward for the last complete JSON object
    }
  }
  throw new Error(`Could not parse trailing JSON payload from output: ${trimmed.slice(-600)}`);
}
function readLastNumericValue(output: string, key: string): number | null {
  const pattern = new RegExp(`"${key}"\\s*:\\s*(-?\\d+)`, "g");
  let match: RegExpExecArray | null = null;
  let lastValue: number | null = null;

  while (true) {
    match = pattern.exec(output);
    if (!match) {
      break;
    }
    lastValue = Number(match[1]);
  }

  return lastValue;
}

function parseProcessFieldJobsOutput(output: string): ProcessFieldJobResult {
  try {
    const payload = parseTrailingJson<Record<string, unknown>>(output);
    const processed = Number(payload.processed ?? 0);
    const requeued = Number(payload.requeued ?? 0);
    const failedTerminal = Number(payload.failed_terminal ?? payload.failedTerminal ?? 0);
    const queueTriage = (payload.queue_triage ?? payload.queueTriage ?? null) as Record<string, unknown> | null;
    const chapterRepair = (payload.chapter_repair ?? payload.chapterRepair ?? null) as Record<string, unknown> | null;
    return {
      processed,
      requeued,
      failedTerminal,
      queueTriage: queueTriage
        ? {
            invalidCancelled: Number(queueTriage.invalidCancelled ?? 0),
            repairQueued: Number(queueTriage.repairQueued ?? 0)
          }
        : undefined,
      chapterRepair: chapterRepair
        ? {
            promotedToCanonical: Number(chapterRepair.promotedToCanonical ?? 0),
            reconciledHistorical: Number(chapterRepair.reconciledHistorical ?? 0)
          }
        : undefined
    };
  } catch {
    const processed = readLastNumericValue(output, "processed");
    const requeued = readLastNumericValue(output, "requeued");
    const failedTerminal = readLastNumericValue(output, "failed_terminal");

    if (processed === null || requeued === null || failedTerminal === null) {
      throw new Error(`Could not parse process-field-jobs output: ${output.slice(-600)}`);
    }

    return { processed, requeued, failedTerminal };
  }
}

async function runAdaptiveCrawlWarmup(config: BenchmarkRunConfig): Promise<CrawlWarmupResult | null> {
  if (!config.sourceSlug) {
    return null;
  }
  if (!config.runAdaptiveCrawlBeforeCycles) {
    return null;
  }

  const runtimeMode = config.crawlRuntimeMode ?? "adaptive_assisted";
  const args =
    runtimeMode === "legacy"
      ? ["-m", "fratfinder_crawler.cli", "run-legacy", "--source-slug", config.sourceSlug]
      : [
          "-m",
          "fratfinder_crawler.cli",
          "run-adaptive",
          "--source-slug",
          config.sourceSlug,
          "--runtime-mode",
          runtimeMode
        ];

  const workingDirectory = findRepositoryRoot();

  return new Promise((resolve, reject) => {
    const child = spawn("python", args, {
      cwd: workingDirectory,
      env: process.env,
      windowsHide: true
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf-8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf-8");
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`crawl warmup exited with code ${code}: ${stderr || stdout}`));
        return;
      }

      resolve({
        runtimeMode,
        recordsSeen: readLastNumericValue(stdout, "records_seen") ?? 0,
        recordsUpserted: readLastNumericValue(stdout, "records_upserted") ?? 0,
        pagesProcessed: readLastNumericValue(stdout, "pages_processed") ?? 0
      });
    });
  });
}

async function runFieldJobCycle(config: BenchmarkRunConfig): Promise<ProcessFieldJobResult> {
  const args = [
    "-m",
    "fratfinder_crawler.cli",
    "process-field-jobs",
    "--limit",
    String(config.limitPerCycle),
    "--workers",
    String(config.workers)
  ];

  if (config.fieldJobRuntimeMode) {
    args.push("--runtime-mode", config.fieldJobRuntimeMode);
  }

  if (config.fieldJobGraphDurability) {
    args.push("--graph-durability", config.fieldJobGraphDurability);
  }

  if (config.fieldName !== "all") {
    args.push("--field-name", config.fieldName);
  }

  if (config.sourceSlug) {
    args.push("--source-slug", config.sourceSlug);
  }

  const timeoutMs = estimateBenchmarkCycleTimeoutMs(config);
  const workingDirectory = findRepositoryRoot();

  return new Promise((resolve, reject) => {
    const child = spawn("python", args, {
      cwd: workingDirectory,
      env: process.env,
      windowsHide: true
    });

    let stdout = "";
    let stderr = "";
    let didTimeout = false;
    let settled = false;

    const settle = (callback: (value: ProcessFieldJobResult | Error) => void, value: ProcessFieldJobResult | Error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      callback(value);
    };

    const forceKillChild = () => {
      if (child.killed) {
        return;
      }
      if (process.platform === "win32" && child.pid) {
        spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
          windowsHide: true,
          stdio: "ignore"
        }).unref();
        return;
      }
      child.kill("SIGKILL");
    };

    const timeout = setTimeout(() => {
      didTimeout = true;
      forceKillChild();
      settle(
        (value) => reject(value as Error),
        new Error(`Benchmark cycle timed out after ${timeoutMs}ms`)
      );
    }, timeoutMs);

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf-8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf-8");
    });

    child.on("error", (error) => {
      settle(
        (value) => reject(value as Error),
        error
      );
    });

    child.on("close", (code) => {
      if (settled) {
        return;
      }
      if (didTimeout) {
        settle(
          (value) => reject(value as Error),
          new Error(`Benchmark cycle timed out after ${timeoutMs}ms`)
        );
        return;
      }

      if (code !== 0) {
        settle(
          (value) => reject(value as Error),
          new Error(`process-field-jobs exited with code ${code}: ${stderr || stdout}`)
        );
        return;
      }

      try {
        settle(
          (value) => resolve(value as ProcessFieldJobResult),
          parseProcessFieldJobsOutput(`${stdout}\n${stderr}`)
        );
      } catch (error) {
        settle(
          (value) => reject(value as Error),
          error instanceof Error ? error : new Error(String(error))
        );
      }
    });
  });
}

function buildSummary(params: {
  startedAtMs: number;
  totals: BenchmarkTotals;
  cyclesCompleted: number;
  startSnapshot: BenchmarkQueueSnapshot;
  endSnapshot: BenchmarkQueueSnapshot;
  executionContext?: BenchmarkExecutionContext;
}): BenchmarkRunSummary {
  const elapsedMs = Math.max(Date.now() - params.startedAtMs, 1);
  const jobsPerMinute = (params.totals.processed * 60_000) / elapsedMs;
  const avgCycleMs = params.cyclesCompleted > 0 ? elapsedMs / params.cyclesCompleted : 0;
  const businessStatus =
    params.totals.processed > 0 ||
    params.totals.invalidBlocked > 0 ||
    params.totals.repairableBlocked > 0 ||
    params.totals.repairPromoted > 0 ||
    params.totals.reconciledHistorical > 0 ||
    params.startSnapshot.queued !== params.endSnapshot.queued
      ? "progressed"
      : "no_business_progress";

  return {
    elapsedMs,
    cyclesCompleted: params.cyclesCompleted,
    totalProcessed: params.totals.processed,
    totalRequeued: params.totals.requeued,
    totalFailedTerminal: params.totals.failedTerminal,
    businessStatus,
    jobsPerMinute,
    avgCycleMs,
    queueDepthStart: params.startSnapshot.queued,
    queueDepthEnd: params.endSnapshot.queued,
    queueDepthDelta: params.startSnapshot.queued - params.endSnapshot.queued,
    invalidBlocked: params.totals.invalidBlocked,
    repairableBlocked: params.totals.repairableBlocked,
    repairPromoted: params.totals.repairPromoted,
    reconciledHistorical: params.totals.reconciledHistorical,
    actionableQueueRemaining: params.endSnapshot.queued,
    preconditions: params.executionContext?.preconditions,
    isolationMode: params.executionContext?.isolationMode,
    contaminationStatus: params.executionContext?.contaminationStatus,
  };
}

export async function runBenchmarkExecution(
  runId: string,
  executionContext?: BenchmarkExecutionContext
): Promise<void> {
  const startedAtMs = Date.now();
  const leaseToken = randomUUID();
  let heartbeat: NodeJS.Timeout | null = null;
  const samples: BenchmarkCycleSample[] = [];
  const totals: BenchmarkTotals = {
    processed: 0,
    requeued: 0,
    failedTerminal: 0,
    invalidBlocked: 0,
    repairableBlocked: 0,
    repairPromoted: 0,
    reconciledHistorical: 0
  };
  let startSnapshot: BenchmarkQueueSnapshot = {
    queued: 0,
    running: 0,
    done: 0,
    failed: 0,
    total: 0
  };
  let endSnapshot = startSnapshot;

  try {
    await upsertRuntimeWorker({
      workerId: BENCHMARK_WORKER_ID,
      workloadLane: "benchmark",
      runtimeOwner: "evaluation_worker_benchmark_runner",
      leaseSeconds: BENCHMARK_WORKER_LEASE_SECONDS,
      metadata: { runId },
    });

    const claimed = await claimBenchmarkRunLease({
      runId,
      workerId: BENCHMARK_WORKER_ID,
      leaseToken,
      leaseSeconds: BENCHMARK_WORKER_LEASE_SECONDS,
    });
    if (!claimed) {
      await stopRuntimeWorker(BENCHMARK_WORKER_ID);
      return;
    }

    heartbeat = setInterval(() => {
      void heartbeatRuntimeWorker(BENCHMARK_WORKER_ID, BENCHMARK_WORKER_LEASE_SECONDS);
      void heartbeatBenchmarkRunLease({
        runId,
        workerId: BENCHMARK_WORKER_ID,
        leaseToken,
        leaseSeconds: BENCHMARK_WORKER_LEASE_SECONDS,
      });
    }, BENCHMARK_HEARTBEAT_INTERVAL_MS);

    await markBenchmarkRunStarted(runId);

    const run = await getBenchmarkRun(runId);
    if (!run) {
      throw new Error(`Benchmark run ${runId} not found`);
    }

    const config = run.config;
    startSnapshot = await getFieldJobStatusSnapshot({
      fieldName: config.fieldName,
      sourceSlug: config.sourceSlug
    });
    endSnapshot = startSnapshot;

    const warmup = await runAdaptiveCrawlWarmup(config);

    for (let cycle = 1; cycle <= config.cycles; cycle += 1) {
      const cycleStartedAt = new Date().toISOString();
      const cycleStartedAtMs = Date.now();
      const result = await runFieldJobCycle(config);
      const cycleFinishedAt = new Date().toISOString();

      totals.processed += result.processed;
      totals.requeued += result.requeued;
      totals.failedTerminal += result.failedTerminal;
      totals.invalidBlocked += result.queueTriage?.invalidCancelled ?? 0;
      totals.repairableBlocked += result.queueTriage?.repairQueued ?? 0;
      totals.repairPromoted += result.chapterRepair?.promotedToCanonical ?? 0;
      totals.reconciledHistorical += result.chapterRepair?.reconciledHistorical ?? 0;

      endSnapshot = await getFieldJobStatusSnapshot({
        fieldName: config.fieldName,
        sourceSlug: config.sourceSlug
      });

      if (isLanggraphRuntime(config.fieldJobRuntimeMode)) {
        try {
          const shadow = await computeBenchmarkShadowDiffWindow({
            sourceSlug: config.sourceSlug,
            fieldName: config.fieldName,
            runtimeMode: config.fieldJobRuntimeMode ?? "langgraph_shadow",
            fromIso: cycleStartedAt,
            toIso: cycleFinishedAt,
          });
          await upsertBenchmarkShadowDiff({
            benchmarkRunId: runId,
            cycle,
            runtimeMode: config.fieldJobRuntimeMode ?? "langgraph_shadow",
            observedJobs: shadow.observedJobs,
            decisionMismatchCount: shadow.decisionMismatchCount,
            statusMismatchCount: shadow.statusMismatchCount,
            mismatchRate: shadow.mismatchRate,
            details: shadow.details,
          });
        } catch {
          // Keep benchmark progress resilient even if shadow-diff persistence fails.
        }
      }

      samples.push({
        cycle,
        startedAt: cycleStartedAt,
        durationMs: Date.now() - cycleStartedAtMs,
        processed: result.processed,
        requeued: result.requeued,
        failedTerminal: result.failedTerminal,
        queued: endSnapshot.queued,
        running: endSnapshot.running,
        done: endSnapshot.done,
        failed: endSnapshot.failed,
        ...(cycle === 1 && warmup
          ? {
              warmupRuntimeMode: warmup.runtimeMode,
              warmupRecordsSeen: warmup.recordsSeen,
              warmupRecordsUpserted: warmup.recordsUpserted,
              warmupPagesProcessed: warmup.pagesProcessed
            }
          : {})
      } as BenchmarkCycleSample);

      if (config.pauseMs > 0 && cycle < config.cycles) {
        await delay(config.pauseMs);
      }
    }

    const summary = buildSummary({
      startedAtMs,
      totals,
      cyclesCompleted: samples.length,
      startSnapshot,
      endSnapshot,
      executionContext,
    });

    await completeBenchmarkRun({
      id: runId,
      summary,
      samples
    });
    if (heartbeat) {
      clearInterval(heartbeat);
      heartbeat = null;
    }
    await releaseBenchmarkRunLease({
      runId,
      workerId: BENCHMARK_WORKER_ID,
      leaseToken,
    });
    await stopRuntimeWorker(BENCHMARK_WORKER_ID);
  } catch (error) {
    try {
      const summary = buildSummary({
        startedAtMs,
        totals,
        cyclesCompleted: samples.length,
        startSnapshot,
        endSnapshot,
        executionContext,
      });

      await failBenchmarkRun({
        id: runId,
        error: error instanceof Error ? error.message : String(error),
        summary,
        samples
      });
    } catch {
      // Keep the scheduler alive even if DB persistence fails unexpectedly.
    }
    if (heartbeat) {
      clearInterval(heartbeat);
      heartbeat = null;
    }
    await releaseBenchmarkRunLease({
      runId,
      workerId: BENCHMARK_WORKER_ID,
      leaseToken,
    });
    await stopRuntimeWorker(BENCHMARK_WORKER_ID);
  }
}

export async function scheduleBenchmarkRun(runId: string): Promise<boolean> {
  if (activeRuns.has(runId)) {
    return false;
  }

  activeRuns.add(runId);
  queueMicrotask(() => {
    void runBenchmarkExecution(runId).finally(() => {
      activeRuns.delete(runId);
    });
  });

  return true;
}

export function isBenchmarkRunActive(runId: string): boolean {
  return activeRuns.has(runId);
}

export function activeBenchmarkRunCount(): number {
  return activeRuns.size;
}

export function toBenchmarkFieldName(value: string): BenchmarkFieldName {
  if (value === "find_website" || value === "find_email" || value === "find_instagram" || value === "all") {
    return value;
  }
  throw new Error(`Unsupported benchmark field name: ${value}`);
}



