import { spawn } from "child_process";
import { randomUUID } from "crypto";
import { existsSync, mkdirSync, writeFileSync } from "fs";
import os from "os";
import path from "path";

import { discoverFraternitySource } from "@/lib/fraternity-discovery";
import { scheduleFraternityCrawlRequest } from "@/lib/fraternity-crawl-request-runner";
import { getAdaptiveInsights } from "@/lib/repositories/adaptive-repository";
import { evaluateSourceUrl } from "@/lib/source-selection";
import {
  appendFraternityCrawlRequestEvent,
  createFraternityCrawlRequest,
  getFraternityCrawlRequest,
  reconcileStaleFraternityCrawlRequests,
  updateFraternityCrawlRequest,
  upsertFraternityRecord,
  upsertSourceRecord
} from "@/lib/repositories/fraternity-crawl-request-repository";
import {
  appendCampaignRunEvent,
  appendRuntimeNote,
  countCampaignRunItemsByStatus,
  emptyCampaignProviderHealthSnapshot,
  getCampaignRun,
  getFieldJobQueueDiagnostics,
  getFieldJobQueueDepth,
  getPreferredCampaignSourceForFraternity,
  getReviewReasonBreakdown,
  getSourceCoverageSnapshot,
  insertCampaignItems,
  listAdaptivePolicySnapshots,
  listQueuedCampaignRunIds,
  reconcileStaleCampaignRuns,
  selectCampaignFraternities,
  updateCampaignRun,
  updateCampaignRunItem
} from "@/lib/repositories/campaign-run-repository";
import {
  claimCampaignRunLease,
  heartbeatCampaignRunLease,
  heartbeatRuntimeWorker,
  releaseCampaignRunLease,
  stopRuntimeWorker,
  upsertRuntimeWorker,
} from "@/lib/repositories/runtime-worker-repository";
import type {
  CampaignProviderHealthHistoryPoint,
  CampaignProviderHealthSnapshot,
  CampaignRun,
  CampaignRunConfig,
  CampaignRunItem,
  CampaignRunSummary,
  CampaignScorecard,
  CampaignAcceptanceGateCheck,
  FraternityCrawlRequest,
  FraternityCrawlRequestConfig,
  FraternityCrawlRequestStage,
  FraternityCrawlRequestStatus
} from "@/lib/types";
import { buildDefaultV4ProgramConfig } from "@/lib/v4-program";

const activeCampaignRuns = new Set<string>();
const CAMPAIGN_WORKER_ID = `campaign-worker:${os.hostname()}:${process.pid}`;
const CAMPAIGN_WORKER_LEASE_SECONDS = Math.max(30, Number(process.env.CAMPAIGN_WORKER_LEASE_SECONDS ?? 120));
const CAMPAIGN_HEARTBEAT_INTERVAL_MS = Math.max(10_000, Math.min(60_000, Math.floor(CAMPAIGN_WORKER_LEASE_SECONDS * 500)));
const TERMINAL_ITEM_STATUSES = new Set(["completed", "failed", "skipped", "canceled"]);
const DEFAULT_POLICY_VERSION = String(process.env.CRAWLER_POLICY_VERSION ?? "adaptive-v1").trim() || "adaptive-v1";
const REVIEW_REASON_PLACEHOLDER = "Chapter record appears to be navigation or placeholder text";
const REVIEW_REASON_NAME_OVERLONG = "Chapter record name exceeded max supported length";
const REVIEW_REASON_SLUG_OVERLONG = "Chapter record slug exceeded max supported length";

interface CommandResult {
  stdout: string;
  stderr: string;
}

interface SearchPreflightResult extends CampaignProviderHealthSnapshot {
  probeOutcomes?: Array<Record<string, unknown>>;
}

interface AdaptiveTrainEvalResult {
  epochs: number;
  runtime_mode: string;
  policy_version: string;
  cohort_label: string;
  report_path: string;
  slope: Record<string, number>;
  rows: Array<{
    epoch: number;
    kpis: Record<string, number>;
  }>;
  unitErrors?: Array<Record<string, unknown>>;
}

interface V4ProgramSnapshot {
  capturedAt: string;
  queueQueued: number;
  oldestQueuedAgeMinutes: number | null;
  placeholderReviewCount: number;
  overlongReviewCount: number;
  delayedRewardEventCount: number;
  delayedRewardTotal: number;
  guardrailHitRate: number;
  validMissingCount: number;
  verifiedWebsiteCount: number;
  topReviewReasons: Array<{ reason: string; count: number }>;
}

function toJsonRecord(value: V4ProgramSnapshot | null): Record<string, unknown> | null {
  return value ? JSON.parse(JSON.stringify(value)) as Record<string, unknown> : null;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

async function runPythonCommand(args: string[], timeoutMs: number): Promise<CommandResult> {
  const workingDirectory = findRepositoryRoot();

  return new Promise((resolve, reject) => {
    const child = spawn("python", args, {
      cwd: workingDirectory,
      env: process.env,
      windowsHide: true
    });

    let stdout = "";
    let stderr = "";
    let settled = false;

    const settle = (callback: (value: CommandResult | Error) => void, value: CommandResult | Error) => {
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
      forceKillChild();
      settle(
        (value) => reject(value as Error),
        new Error(`Command timed out after ${timeoutMs}ms: python ${args.join(" ")}`)
      );
    }, timeoutMs);

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf-8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf-8");
    });

    child.on("error", (error) => {
      settle((value) => reject(value as Error), error);
    });

    child.on("close", (code) => {
      if (settled) {
        return;
      }
      if (code !== 0) {
        settle(
          (value) => reject(value as Error),
          new Error(`python ${args.join(" ")} exited with code ${code}: ${stderr || stdout}`)
        );
        return;
      }
      settle((value) => resolve(value as CommandResult), { stdout, stderr });
    });
  });
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

function buildDefaultRequestConfig(
  campaignConfig: CampaignRunConfig,
  concurrency: number,
  crawlPolicyVersion: string | null = null
): FraternityCrawlRequestConfig {
  const workers = Math.max(3, Math.min(12, concurrency <= 1 ? 5 : 8));
  const limitPerCycle = Math.max(30, Math.min(120, concurrency <= 1 ? 40 : 60));
  return {
    fieldJobWorkers: workers,
    fieldJobLimitPerCycle: limitPerCycle,
    maxEnrichmentCycles: Math.max(18, Math.min(60, Math.round(campaignConfig.maxDurationMinutes / 3.5))),
    pauseMs: 750,
    crawlPolicyVersion,
  };
}

function appendProviderHealthHistory(
  history: CampaignProviderHealthHistoryPoint[] | undefined,
  snapshot: CampaignProviderHealthSnapshot,
  activeConcurrency: number,
  queueDepth: number,
  timestamp = new Date().toISOString()
): CampaignProviderHealthHistoryPoint[] {
  return [
    ...(history ?? []),
    {
      ...snapshot,
      timestamp,
      activeConcurrency,
      queueDepth
    }
  ].slice(-48);
}

function requestStatusToItemStatus(status: FraternityCrawlRequestStatus): CampaignRunItem["status"] {
  if (status === "queued") {
    return "queued";
  }
  if (status === "running") {
    return "running";
  }
  if (status === "succeeded") {
    return "completed";
  }
  if (status === "failed") {
    return "failed";
  }
  if (status === "canceled") {
    return "canceled";
  }
  return "request_created";
}

function compactHistogram(entries: Array<{ reason: string; count: number }>): Array<{ reason: string; count: number }> {
  const counts = new Map<string, number>();
  for (const entry of entries) {
    counts.set(entry.reason, (counts.get(entry.reason) ?? 0) + entry.count);
  }
  return [...counts.entries()]
    .map(([reason, count]) => ({ reason, count }))
    .sort((left, right) => right.count - left.count)
    .slice(0, 8);
}

function resolveCampaignStartMs(run: CampaignRun): number | null {
  const firstStartEvent = [...run.events]
    .filter((event) => event.eventType === "campaign_started")
    .sort((left, right) => new Date(left.createdAt).getTime() - new Date(right.createdAt).getTime())[0];

  if (firstStartEvent) {
    return new Date(firstStartEvent.createdAt).getTime();
  }

  return run.startedAt ? new Date(run.startedAt).getTime() : null;
}

function computeScorecard(params: {
  item: CampaignRunItem;
  request: FraternityCrawlRequest;
  coverage: Awaited<ReturnType<typeof getSourceCoverageSnapshot>>;
}): CampaignScorecard {
  let processedJobs = 0;
  let requeuedJobs = 0;
  let failedTerminalJobs = 0;
  const failureEntries: Array<{ reason: string; count: number }> = [];

  for (const event of params.request.events) {
    if (event.eventType === "enrichment_cycle") {
      processedJobs += Number(event.payload.processed ?? 0);
      requeuedJobs += Number(event.payload.requeued ?? 0);
      failedTerminalJobs += Number(event.payload.failedTerminal ?? 0);
    }
    if (event.eventType === "request_failed" || event.eventType === "stage_failed") {
      const reason = String(event.payload.error ?? params.request.lastError ?? event.message ?? "request_failed");
      failureEntries.push({ reason, count: 1 });
    }
  }

  if (params.request.lastError) {
    failureEntries.push({ reason: params.request.lastError, count: 1 });
  }

  const baselineTotalChapters = Number(params.item.scorecard.baselineTotalChapters ?? 0);
  const baselineWebsitesFound = Number(params.item.scorecard.baselineWebsitesFound ?? 0);
  const baselineEmailsFound = Number(params.item.scorecard.baselineEmailsFound ?? 0);
  const baselineInstagramsFound = Number(params.item.scorecard.baselineInstagramsFound ?? 0);
  const baselineChaptersWithAnyContact = Number(params.item.scorecard.baselineChaptersWithAnyContact ?? 0);
  const baselineChaptersWithAllThree = Number(params.item.scorecard.baselineChaptersWithAllThree ?? 0);

  const chaptersDiscovered = Number(params.request.progress.crawlRun?.recordsSeen ?? 0);
  const fieldJobsCreated = Number(params.request.progress.crawlRun?.fieldJobsCreated ?? 0);
  const reviewItemsCreated = Number(params.request.progress.crawlRun?.reviewItemsCreated ?? 0);
  const websitesFound = Math.max(0, params.coverage.websitesFound - baselineWebsitesFound);
  const emailsFound = Math.max(0, params.coverage.emailsFound - baselineEmailsFound);
  const instagramsFound = Math.max(0, params.coverage.instagramsFound - baselineInstagramsFound);
  const chaptersWithAnyContact = Math.max(0, params.coverage.chaptersWithAnyContact - baselineChaptersWithAnyContact);
  const chaptersWithAllThree = Math.max(0, params.coverage.chaptersWithAllThree - baselineChaptersWithAllThree);
  const totalChapters = Math.max(chaptersDiscovered, Math.max(0, params.coverage.totalChapters - baselineTotalChapters), 1);
  const foundContacts = websitesFound + emailsFound + instagramsFound;
  const totalJobEvents = Math.max(processedJobs + requeuedJobs + failedTerminalJobs, 1);
  const completedContactChapters = chaptersWithAnyContact;
  const sourceNativeYield = Math.min(1, chaptersDiscovered / Math.max(fieldJobsCreated, chaptersDiscovered, 1));
  const searchEfficiency = foundContacts / totalJobEvents;
  const retryEfficiency = completedContactChapters / Math.max(requeuedJobs, 1);
  const confidenceQuality = completedContactChapters / totalChapters;
  const providerResilience = params.request.status === "succeeded" ? 1 : params.request.status === "running" ? 0.65 : 0.35;
  const queueEfficiency = processedJobs / totalJobEvents;
  const sourceProvenance = params.request.progress.discovery?.sourceProvenance ?? "unknown";

  return {
    baselineTotalChapters,
    baselineWebsitesFound,
    baselineEmailsFound,
    baselineInstagramsFound,
    baselineChaptersWithAnyContact,
    baselineChaptersWithAllThree,
    chaptersDiscovered,
    fieldJobsCreated,
    processedJobs,
    requeuedJobs,
    failedTerminalJobs,
    reviewItemsCreated,
    websitesFound,
    emailsFound,
    instagramsFound,
    chaptersWithAnyContact,
    chaptersWithAllThree,
    sourceNativeYield,
    searchEfficiency,
    retryEfficiency,
    confidenceQuality,
    providerResilience,
    queueEfficiency,
    providerAttempts: {
      [sourceProvenance]: 1
    },
    failureHistogram: compactHistogram(failureEntries)
  };
}

function buildSummary(params: {
  run: CampaignRun;
  queueDepthStart: number;
  queueDepthEnd: number;
  counts: Record<string, number>;
}): CampaignRunSummary {
  const startedAtMs = resolveCampaignStartMs(params.run);
  const durationMs = Math.max((startedAtMs ? Date.now() - startedAtMs : 0), 1);

  let totalChapters = 0;
  let websitesFound = 0;
  let emailsFound = 0;
  let instagramsFound = 0;
  let chaptersWithAnyContact = 0;
  let chaptersWithAllThree = 0;
  let totalProcessed = 0;
  let totalRequeued = 0;
  let totalFailedTerminal = 0;

  for (const item of params.run.items) {
    totalChapters += Math.max(item.scorecard.chaptersDiscovered, item.scorecard.chaptersWithAnyContact, item.scorecard.chaptersWithAllThree);
    websitesFound += item.scorecard.websitesFound;
    emailsFound += item.scorecard.emailsFound;
    instagramsFound += item.scorecard.instagramsFound;
    chaptersWithAnyContact += item.scorecard.chaptersWithAnyContact;
    chaptersWithAllThree += item.scorecard.chaptersWithAllThree;
    totalProcessed += item.scorecard.processedJobs;
    totalRequeued += item.scorecard.requeuedJobs;
    totalFailedTerminal += item.scorecard.failedTerminalJobs;
  }

  const denominator = Math.max(totalChapters, 1);
  const queueDepthDelta = params.queueDepthStart - params.queueDepthEnd;
  const businessStatus =
    totalProcessed > 0 ||
    totalRequeued > 0 ||
    totalFailedTerminal > 0 ||
    queueDepthDelta > 0 ||
    Number(params.counts.completed ?? 0) > 0
      ? "progressed"
      : "no_business_progress";
  return {
    targetCount: params.run.config.targetCount,
    itemCount: params.run.items.length,
    completedCount: Number(params.counts.completed ?? 0),
    failedCount: Number(params.counts.failed ?? 0),
    skippedCount: Number(params.counts.skipped ?? 0),
    activeCount: Number(params.counts.running ?? 0) + Number(params.counts.queued ?? 0) + Number(params.counts.request_created ?? 0),
    businessStatus,
    anyContactSuccessRate: chaptersWithAnyContact / denominator,
    allThreeSuccessRate: chaptersWithAllThree / denominator,
    websiteCoverageRate: websitesFound / denominator,
    emailCoverageRate: emailsFound / denominator,
    instagramCoverageRate: instagramsFound / denominator,
    jobsPerMinute: (totalProcessed * 60_000) / durationMs,
    queueDepthStart: params.queueDepthStart,
    queueDepthEnd: params.queueDepthEnd,
    queueDepthDelta,
    totalProcessed,
    totalRequeued,
    totalFailedTerminal,
    durationMs,
    checkpointCount: Number(params.run.summary.checkpointCount ?? 0)
  };
}

async function runSearchPreflight(probes = 4): Promise<SearchPreflightResult> {
  const result = await runPythonCommand(["-m", "fratfinder_crawler.cli", "search-preflight", "--probes", String(probes)], 120_000);
  const payload = parseTrailingJson<Record<string, unknown>>(`${result.stdout}\n${result.stderr}`);
  return {
    healthy: Boolean(payload.healthy),
    successRate: Number(payload.successRate ?? payload.success_rate ?? 0),
    probes: Number(payload.probes ?? 0),
    successes: Number(payload.successes ?? 0),
    minSuccessRate: Number(payload.minSuccessRate ?? payload.min_success_rate ?? 0),
    providerHealth:
      payload.providerHealth && typeof payload.providerHealth === "object"
        ? (payload.providerHealth as Record<string, Record<string, number>>)
        : payload.provider_health && typeof payload.provider_health === "object"
          ? (payload.provider_health as Record<string, Record<string, number>>)
          : {},
    probeOutcomes: Array.isArray(payload.probeOutcomes)
      ? (payload.probeOutcomes as Array<Record<string, unknown>>)
      : Array.isArray(payload.probe_outcomes)
        ? (payload.probe_outcomes as Array<Record<string, unknown>>)
        : []
  };
}

function resolveReportPath(relativePath: string): string {
  return path.join(findRepositoryRoot(), relativePath);
}

function sumReviewReasons(
  reasons: Array<{ reason: string; count: number }>,
  targets: string[]
): number {
  const targetSet = new Set(targets.map((item) => item.toLowerCase()));
  return reasons
    .filter((item) => targetSet.has(item.reason.toLowerCase()))
    .reduce((total, item) => total + item.count, 0);
}

async function captureV4ProgramSnapshot(sourceSlugs: string[], windowDays: number, createdAfter?: string | null): Promise<V4ProgramSnapshot> {
  const [queueDiagnostics, reviewReasons, adaptiveInsights] = await Promise.all([
    getFieldJobQueueDiagnostics(sourceSlugs),
    getReviewReasonBreakdown({ sourceSlugs, windowDays, createdAfter, limit: 20 }),
    getAdaptiveInsights({ sourceSlugs, runtimeMode: "adaptive_primary", windowDays, limit: 25 }),
  ]);

  return {
    capturedAt: new Date().toISOString(),
    queueQueued: queueDiagnostics.queuedTotal,
    oldestQueuedAgeMinutes: queueDiagnostics.oldestQueuedAgeMinutes,
    placeholderReviewCount: sumReviewReasons(reviewReasons, [REVIEW_REASON_PLACEHOLDER]),
    overlongReviewCount: sumReviewReasons(reviewReasons, [REVIEW_REASON_NAME_OVERLONG, REVIEW_REASON_SLUG_OVERLONG]),
    delayedRewardEventCount: adaptiveInsights.delayedRewardEventCount,
    delayedRewardTotal: adaptiveInsights.delayedRewardTotal,
    guardrailHitRate: adaptiveInsights.guardrailHitRate,
    validMissingCount: adaptiveInsights.validMissingCount,
    verifiedWebsiteCount: adaptiveInsights.verifiedWebsiteCount,
    topReviewReasons: adaptiveInsights.topReviewReasons,
  };
}

async function runAdaptiveTrainEval(params: {
  runId: string;
  round: number;
  trainSourceSlugs: string[];
  evalSourceSlugs: string[];
  runtimeMode: string;
  policyVersion: string;
  epochsPerRound: number;
  commandTimeoutMinutes?: number;
}): Promise<AdaptiveTrainEvalResult> {
  const reportPath = `docs/reports/V4_RL_PROGRAM_${params.runId}_ROUND_${String(params.round).padStart(2, "0")}.md`;
  const command = [
    "-m",
    "fratfinder_crawler.cli",
    "adaptive-train-eval",
    "--epochs",
    String(params.epochsPerRound),
    "--train-sources",
    params.trainSourceSlugs.join(","),
    "--eval-sources",
    params.evalSourceSlugs.join(","),
    "--runtime-mode",
    params.runtimeMode,
    "--cohort-label",
    `v4-program-${params.runId}`,
    "--policy-version",
    params.policyVersion,
    "--report-path",
    reportPath,
  ];
  const timeoutMinutes = Math.max(5, Math.floor(params.commandTimeoutMinutes ?? 30));
  const output = await runPythonCommand(command, timeoutMinutes * 60_000);
  const payload = parseTrailingJson<AdaptiveTrainEvalResult>(`${output.stdout}\n${output.stderr}`);
  return {
    ...payload,
    report_path: payload.report_path || reportPath,
  };
}

async function runAdaptiveTrainEvalIsolated(params: {
  runId: string;
  round: number;
  trainSourceSlugs: string[];
  evalSourceSlugs: string[];
  runtimeMode: string;
  policyVersion: string;
  epochsPerRound: number;
  commandTimeoutMinutes?: number;
  campaignRunId: string;
}): Promise<AdaptiveTrainEvalResult> {
  const unitResults: AdaptiveTrainEvalResult[] = [];
  const unitErrors: Array<Record<string, unknown>> = [];
  const evalUnits = (params.evalSourceSlugs.length > 0 ? params.evalSourceSlugs : params.trainSourceSlugs).filter(Boolean);

  for (let index = 0; index < evalUnits.length; index += 1) {
    const evalSource = evalUnits[index];
    if (!evalSource) {
      continue;
    }
    const trainSource =
      params.trainSourceSlugs[index % Math.max(1, params.trainSourceSlugs.length)] ?? evalSource;
    const unitPolicyVersion = `${params.policyVersion}-u${index + 1}`;

    try {
      const unitResult = await runAdaptiveTrainEval({
        runId: params.runId,
        round: params.round,
        trainSourceSlugs: [trainSource],
        evalSourceSlugs: [evalSource],
        runtimeMode: params.runtimeMode,
        policyVersion: unitPolicyVersion,
        epochsPerRound: params.epochsPerRound,
        commandTimeoutMinutes: params.commandTimeoutMinutes,
      });
      unitResults.push(unitResult);
      await appendCampaignRunEvent({
        campaignRunId: params.campaignRunId,
        eventType: "training_round_source_completed",
        message: `Completed isolated training/eval unit for ${evalSource}`,
        payload: {
          round: params.round,
          trainSource,
          evalSource,
          policyVersion: unitPolicyVersion,
          reportPath: unitResult.report_path,
          kpis: lastRoundKpis(unitResult),
        },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      unitErrors.push({
        trainSource,
        evalSource,
        policyVersion: unitPolicyVersion,
        error: message,
      });
      await appendCampaignRunEvent({
        campaignRunId: params.campaignRunId,
        eventType: "training_round_source_failed",
        message: `Isolated training/eval unit failed for ${evalSource}`,
        payload: {
          round: params.round,
          trainSource,
          evalSource,
          policyVersion: unitPolicyVersion,
          error: message,
        },
      });
    }
  }

  if (unitResults.length === 0) {
    throw new Error(`All isolated training units failed for round ${params.round}`);
  }

  const finalRows = unitResults
    .map((result) => result.rows[result.rows.length - 1])
    .filter((row): row is AdaptiveTrainEvalResult["rows"][number] => Boolean(row))
    .map((row, index) => ({ epoch: index + 1, kpis: row.kpis }));

  const slopeSums = new Map<string, number>();
  for (const result of unitResults) {
    for (const [key, value] of Object.entries(result.slope ?? {})) {
      slopeSums.set(key, (slopeSums.get(key) ?? 0) + Number(value ?? 0));
    }
  }
  const averagedSlope = Object.fromEntries(
    [...slopeSums.entries()].map(([key, value]) => [key, value / unitResults.length])
  );

  const latest = unitResults[unitResults.length - 1];
  if (!latest) {
    throw new Error(`Missing isolated training result for round ${params.round}`);
  }
  return {
    ...latest,
    epochs: unitResults.length,
    policy_version: params.policyVersion,
    report_path: latest.report_path,
    slope: averagedSlope,
    rows: finalRows,
    unitErrors,
  };
}

function selectRoundSources(
  sourceSlugs: string[],
  round: number,
  totalRounds: number,
  explicitBatchSize?: number
): string[] {
  if (sourceSlugs.length === 0) {
    return [];
  }

  const maxBatchSize = sourceSlugs.length;
  const computedBatchSize = Math.max(1, Math.ceil(sourceSlugs.length / Math.max(1, totalRounds)));
  const batchSize = Math.min(
    maxBatchSize,
    Math.max(1, Math.floor(explicitBatchSize ?? computedBatchSize))
  );
  const startIndex = ((Math.max(1, round) - 1) * batchSize) % sourceSlugs.length;
  const selected: string[] = [];
  for (let index = 0; index < batchSize; index += 1) {
    const slug = sourceSlugs[(startIndex + index) % sourceSlugs.length];
    if (!slug) {
      continue;
    }
    if (!selected.includes(slug)) {
      selected.push(slug);
    }
  }
  return selected;
}

function lastRoundKpis(result: AdaptiveTrainEvalResult): Record<string, number> {
  return result.rows[result.rows.length - 1]?.kpis ?? {};
}

function computePromotionReason(params: {
  kpis: Record<string, number>;
  baseline: V4ProgramSnapshot;
  current: V4ProgramSnapshot;
  bestBalancedScore: number;
}): { promoted: boolean; reason: string } {
  const balancedScore = Number(params.kpis.balancedScore ?? 0);
  const improvedBalanced = balancedScore > params.bestBalancedScore + 0.0001;
  const queueDidNotWorsen = params.current.queueQueued <= params.baseline.queueQueued;
  const reviewDidNotWorsen =
    params.current.placeholderReviewCount <= params.baseline.placeholderReviewCount &&
    params.current.overlongReviewCount <= params.baseline.overlongReviewCount;
  if (improvedBalanced && queueDidNotWorsen && reviewDidNotWorsen) {
    return { promoted: true, reason: "balanced_score_queue_and_review_gates_passed" };
  }
  if (!improvedBalanced) {
    return { promoted: false, reason: "balanced_score_not_improved" };
  }
  if (!queueDidNotWorsen) {
    return { promoted: false, reason: "queue_efficiency_regressed" };
  }
  return { promoted: false, reason: "precision_safety_regressed" };
}

function driftFromSnapshots(
  baseline: V4ProgramSnapshot,
  current: V4ProgramSnapshot
): Array<{ reason: string; baselineCount: number; latestCount: number; delta: number }> {
  const reasons = new Map<string, number>();
  for (const entry of baseline.topReviewReasons) {
    reasons.set(entry.reason, 0);
  }
  for (const entry of current.topReviewReasons) {
    reasons.set(entry.reason, 0);
  }
  return [...reasons.keys()].map((reason) => ({
    reason,
    baselineCount: baseline.topReviewReasons.find((item) => item.reason === reason)?.count ?? 0,
    latestCount: current.topReviewReasons.find((item) => item.reason === reason)?.count ?? 0,
    delta:
      (current.topReviewReasons.find((item) => item.reason === reason)?.count ?? 0) -
      (baseline.topReviewReasons.find((item) => item.reason === reason)?.count ?? 0),
  })).sort((left, right) => right.delta - left.delta);
}

function buildAcceptanceGate(params: {
  run: CampaignRun;
  baseline: V4ProgramSnapshot | null;
  finalSnapshot: V4ProgramSnapshot;
}): { passed: boolean; checks: CampaignAcceptanceGateCheck[] } {
  const completed = params.run.items.filter((item) => TERMINAL_ITEM_STATUSES.has(item.status)).length;
  const succeeded = params.run.items.filter((item) => item.status === "completed").length;
  const failed = params.run.items.filter((item) => item.status === "failed").length;
  const totalRecordsSeen = params.run.items.reduce((total, item) => total + Math.max(item.scorecard.chaptersDiscovered, 0), 0);
  const lowConfidenceRate =
    totalRecordsSeen > 0
      ? (params.finalSnapshot.placeholderReviewCount + params.finalSnapshot.overlongReviewCount) / totalRecordsSeen
      : 0;
  const queueImprovement = params.baseline
    ? params.baseline.queueQueued > 0
      ? (params.baseline.queueQueued - params.finalSnapshot.queueQueued) / params.baseline.queueQueued
      : 0
    : 0;
  const checks: CampaignAcceptanceGateCheck[] = [
    {
      label: "Terminal requests",
      value: `${completed}/${params.run.items.length}`,
      target: `${params.run.items.length}/${params.run.items.length}`,
      passed: completed === params.run.items.length,
    },
    {
      label: "Succeeded requests",
      value: `${succeeded}`,
      target: ">= 18",
      passed: succeeded >= 18,
    },
    {
      label: "Failed requests",
      value: `${failed}`,
      target: "= 0",
      passed: failed === 0,
    },
    {
      label: "Low-confidence review rate",
      value: `${(lowConfidenceRate * 100).toFixed(2)}%`,
      target: "< 3.00%",
      passed: lowConfidenceRate < 0.03,
    },
    {
      label: "Queue improvement",
      value: `${(queueImprovement * 100).toFixed(1)}%`,
      target: ">= 50.0%",
      passed: queueImprovement >= 0.5,
    },
    {
      label: "Delayed rewards present",
      value: `${params.finalSnapshot.delayedRewardEventCount}`,
      target: "> 0",
      passed: params.finalSnapshot.delayedRewardEventCount > 0,
    },
  ];
  return {
    passed: checks.every((check) => check.passed),
    checks,
  };
}

function writeV4FinalReport(params: {
  run: CampaignRun;
  baseline: V4ProgramSnapshot | null;
  finalSnapshot: V4ProgramSnapshot;
  reportPath: string;
}): void {
  const promotionDecisions = params.run.telemetry.promotionDecisions ?? [];
  const lines = [
    `# V4 RL Improvement Report (${new Date().toISOString()})`,
    "",
    `- Campaign: \`${params.run.name}\``,
    `- Campaign ID: \`${params.run.id}\``,
    `- Active policy version: \`${params.run.telemetry.activePolicyVersion ?? DEFAULT_POLICY_VERSION}\``,
    `- Active policy snapshot: \`${params.run.telemetry.activePolicySnapshotId ?? "n/a"}\``,
    "",
    "## Baseline",
    "```json",
    JSON.stringify(params.baseline, null, 2),
    "```",
    "",
    "## Final Snapshot",
    "```json",
    JSON.stringify(params.finalSnapshot, null, 2),
    "```",
    "",
    "## Promotion Decisions",
    "```json",
    JSON.stringify(promotionDecisions, null, 2),
    "```",
    "",
    "## Remaining Failure Modes",
    "```json",
    JSON.stringify(params.run.telemetry.reviewReasonDrift ?? [], null, 2),
    "```",
  ];
  const absolutePath = resolveReportPath(params.reportPath);
  mkdirSync(path.dirname(absolutePath), { recursive: true });
  writeFileSync(absolutePath, `${lines.join("\n")}\n`, "utf-8");
}

async function ensureCampaignItems(run: CampaignRun): Promise<CampaignRun> {
  if (run.items.length > 0) {
    return run;
  }

  const selected = await selectCampaignFraternities(run.config);
  if (selected.length === 0) {
    throw new Error("No eligible fraternities were available to seed this campaign.");
  }

  await insertCampaignItems(run.id, selected);
  await appendCampaignRunEvent({
    campaignRunId: run.id,
    eventType: "campaign_seeded",
    message: `Seeded campaign with ${selected.length} fraternities`,
    payload: {
      selected: selected.map((item) => ({
        fraternitySlug: item.fraternitySlug,
        fraternityName: item.fraternityName,
        cohort: item.cohort,
        selectionReason: item.selectionReason
      }))
    }
  });

  const refreshed = await getCampaignRun(run.id);
  if (!refreshed) {
    throw new Error(`Campaign run ${run.id} disappeared after seeding`);
  }
  return refreshed;
}

async function createRequestForItem(run: CampaignRun, item: CampaignRunItem, concurrency: number): Promise<void> {
  const preferredSource = await getPreferredCampaignSourceForFraternity(item.fraternitySlug);
  if (!preferredSource?.sourceUrl) {
    await updateCampaignRunItem({
      id: item.id,
      status: "failed",
      notes: "Missing active verified source for campaign item"
    });
    await appendCampaignRunEvent({
      campaignRunId: run.id,
      eventType: "item_failed",
      message: `Could not create request for ${item.fraternityName}`,
      payload: { fraternitySlug: item.fraternitySlug, reason: "missing_verified_source" }
    });
    return;
  }

  let selectedSource = preferredSource;
  const preferredQuality = evaluateSourceUrl(preferredSource.sourceUrl);
  if (preferredQuality.isWeak || preferredQuality.score < 0.58) {
    try {
      const rediscovered = await discoverFraternitySource(item.fraternityName);
      if (rediscovered.selectedUrl) {
        const rediscoveredQuality = evaluateSourceUrl(rediscovered.selectedUrl);
        if (rediscoveredQuality.score > preferredQuality.score + 0.12) {
          selectedSource = {
            fraternityName: rediscovered.fraternityName,
            fraternitySlug: rediscovered.fraternitySlug,
            sourceSlug: `${rediscovered.fraternitySlug}-main`,
            sourceUrl: rediscovered.selectedUrl,
            confidence: rediscovered.selectedConfidence,
            sourceProvenance: rediscovered.sourceProvenance ?? "search",
            selectionReason: `rediscovered_campaign_source:${preferredSource.selectionReason}`
          };
          await appendCampaignRunEvent({
            campaignRunId: run.id,
            eventType: "item_source_upgraded",
            message: `Upgraded source for ${item.fraternityName} before request creation`,
            payload: {
              fraternitySlug: item.fraternitySlug,
              previousUrl: preferredSource.sourceUrl,
              previousQuality: preferredQuality,
              nextUrl: rediscovered.selectedUrl,
              nextQuality: rediscoveredQuality,
              sourceProvenance: rediscovered.sourceProvenance,
              fallbackReason: rediscovered.fallbackReason
            }
          });
        }
      }
    } catch {
      // Keep the preferred source when rediscovery cannot improve the decision.
    }
  }

  const fraternityRecord = await upsertFraternityRecord({
    slug: selectedSource.fraternitySlug,
    name: selectedSource.fraternityName,
    nicAffiliated: true
  });

  const url = new URL(selectedSource.sourceUrl);
  const sourceSlug = selectedSource.sourceSlug;
  const selectedQuality = evaluateSourceUrl(selectedSource.sourceUrl);

  await upsertSourceRecord({
    fraternityId: fraternityRecord.id,
    slug: sourceSlug,
    baseUrl: url.origin,
    listPath: selectedSource.sourceUrl,
    sourceType: "html_directory",
    parserKey: "directory_v1",
    active: true,
    metadata: {
      discovery: {
        selectedUrl: selectedSource.sourceUrl,
        selectedConfidence: selectedSource.confidence,
        confidenceTier: selectedSource.confidence >= 0.8 ? "high" : selectedSource.confidence >= 0.6 ? "medium" : "low",
        sourceProvenance: selectedSource.sourceProvenance,
        fallbackReason: null,
        resolutionTrace: [
          {
            step: selectedSource.sourceProvenance === "existing_source" ? "existing_source" : "verified_sources",
            selected: true,
            confidence: selectedSource.confidence,
            url: selectedSource.sourceUrl,
            selectionReason: selectedSource.selectionReason
          }
        ],
        sourceQuality: selectedQuality
      }
    }
  });

  const baseline = await getSourceCoverageSnapshot(sourceSlug);
  const config = buildDefaultRequestConfig(
    run.config,
    concurrency,
    run.telemetry.activePolicyVersion ?? DEFAULT_POLICY_VERSION
  );
  const request = await createFraternityCrawlRequest({
    fraternityName: selectedSource.fraternityName,
    fraternitySlug: selectedSource.fraternitySlug,
    sourceSlug,
    sourceUrl: selectedSource.sourceUrl,
    sourceConfidence: selectedSource.confidence,
    status: "queued",
    stage: "discovery",
    scheduledFor: new Date().toISOString(),
    priority: item.cohort === "control" ? 50 : 25,
    config,
    progress: {
      discovery: {
        sourceUrl: selectedSource.sourceUrl,
        sourceConfidence: selectedSource.confidence,
        confidenceTier: selectedSource.confidence >= 0.8 ? "high" : selectedSource.confidence >= 0.6 ? "medium" : "low",
        sourceProvenance: selectedSource.sourceProvenance,
        fallbackReason: null,
        resolutionTrace: [
          {
            step: selectedSource.sourceProvenance === "existing_source" ? "existing_source" : "verified_sources",
            selected: true,
            reason: `${item.selectionReason}:${selectedSource.selectionReason}`,
            confidence: selectedSource.confidence,
            url: selectedSource.sourceUrl
          }
        ],
        candidates: []
      },
      analytics: {
        sourceQuality: {
          score: selectedQuality.score,
          isWeak: selectedQuality.isWeak,
          reasons: selectedQuality.reasons,
          recoveryAttempts: 0,
          recoveredFromUrl: preferredSource.sourceUrl !== selectedSource.sourceUrl ? preferredSource.sourceUrl : null,
          recoveredToUrl: preferredSource.sourceUrl !== selectedSource.sourceUrl ? selectedSource.sourceUrl : null
        }
      }
    },
    lastError: null
  });

  await appendFraternityCrawlRequestEvent({
    requestId: request.id,
    eventType: "request_created",
    message: `Campaign request created for ${selectedSource.fraternityName}`,
    payload: {
      campaignRunId: run.id,
      campaignItemId: item.id,
      sourceSlug,
      sourceUrl: selectedSource.sourceUrl,
      sourceProvenance: selectedSource.sourceProvenance,
      cohort: item.cohort,
      baselineCoverage: baseline,
      selectionReason: selectedSource.selectionReason,
      sourceQuality: selectedQuality,
      policyVersion: config.crawlPolicyVersion
    }
  });

  await appendFraternityCrawlRequestEvent({
    requestId: request.id,
    eventType: "request_queued",
    message: "Campaign request queued for staged crawl execution",
    payload: {
      campaignRunId: run.id,
      scheduledFor: request.scheduledFor
    }
  });

  await updateCampaignRunItem({
    id: item.id,
    requestId: request.id,
    status: "queued",
    notes: `Source ${sourceSlug} admitted from verified registry`,
    scorecard: {
      ...item.scorecard,
      baselineTotalChapters: baseline.totalChapters,
      baselineWebsitesFound: baseline.websitesFound,
      baselineEmailsFound: baseline.emailsFound,
      baselineInstagramsFound: baseline.instagramsFound,
      baselineChaptersWithAnyContact: baseline.chaptersWithAnyContact,
      baselineChaptersWithAllThree: baseline.chaptersWithAllThree
    }
  });

  await appendCampaignRunEvent({
    campaignRunId: run.id,
    eventType: "item_admitted",
    message: `Admitted ${item.fraternityName} into the active campaign window`,
    payload: {
      fraternitySlug: item.fraternitySlug,
      requestId: request.id,
      cohort: item.cohort,
      sourceSlug,
      baselineCoverage: baseline,
      sourceProvenance: selectedSource.sourceProvenance,
      selectionReason: selectedSource.selectionReason,
      policyVersion: config.crawlPolicyVersion
    }
  });

  await scheduleFraternityCrawlRequest(request.id);
}

async function syncCampaignItems(run: CampaignRun): Promise<CampaignRun> {
  for (const item of run.items) {
    if (!item.requestId) {
      continue;
    }

    const request = await getFraternityCrawlRequest(item.requestId);
    if (!request) {
      if (item.status !== "failed") {
        await updateCampaignRunItem({
          id: item.id,
          status: "failed",
          notes: "Linked request was not found during campaign reconciliation"
        });
      }
      continue;
    }

    const coverage = request.sourceSlug ? await getSourceCoverageSnapshot(request.sourceSlug) : {
      totalChapters: 0,
      websitesFound: 0,
      emailsFound: 0,
      instagramsFound: 0,
      chaptersWithAnyContact: 0,
      chaptersWithAllThree: 0
    };

    const nextScorecard = computeScorecard({
      item,
      request,
      coverage
    });

    const nextStatus = requestStatusToItemStatus(request.status);
    const nextNotes = request.lastError ?? item.notes;

    await updateCampaignRunItem({
      id: item.id,
      status: nextStatus,
      scorecard: nextScorecard,
      notes: nextNotes
    });
  }

  const refreshed = await getCampaignRun(run.id);
  if (!refreshed) {
    throw new Error(`Campaign run ${run.id} disappeared during sync`);
  }
  return refreshed;
}

async function maybeEmitCheckpoint(run: CampaignRun, queueDepthStart: number, lastCheckpointAtMs: number): Promise<number> {
  const now = Date.now();
  if (now - lastCheckpointAtMs < run.config.checkpointIntervalMs) {
    return lastCheckpointAtMs;
  }

  const queueDepthEnd = await getFieldJobQueueDepth();
  const counts = await countCampaignRunItemsByStatus(run.id);
  const summary = buildSummary({
    run,
    queueDepthStart,
    queueDepthEnd,
    counts
  });
  summary.checkpointCount += 1;

  await updateCampaignRun({
    id: run.id,
    summary,
    telemetry: {
      ...run.telemetry,
      lastCheckpointAt: new Date(now).toISOString()
    }
  });

  await appendCampaignRunEvent({
    campaignRunId: run.id,
    eventType: "checkpoint",
    message: `Checkpoint ${summary.checkpointCount} captured`,
    payload: {
      summary,
      queueDepthEnd,
      counts,
      providerHealth: run.telemetry.providerHealth ?? null
    }
  });

  return now;
}

async function refreshRunningSummary(run: CampaignRun, queueDepthStart: number): Promise<CampaignRun> {
  const queueDepthEnd = await getFieldJobQueueDepth();
  const counts = await countCampaignRunItemsByStatus(run.id);
  const summary = buildSummary({
    run,
    queueDepthStart,
    queueDepthEnd,
    counts
  });

  await updateCampaignRun({
    id: run.id,
    summary
  });

  return (await getCampaignRun(run.id)) ?? run;
}

async function maybeTuneCampaign(run: CampaignRun, lastTuneAtMs: number): Promise<number> {
  const now = Date.now();
  if (!run.config.autoTuningEnabled || now - lastTuneAtMs < run.config.tuningIntervalMs) {
    return lastTuneAtMs;
  }

  let health: SearchPreflightResult;
  try {
    health = await runSearchPreflight(3);
  } catch (error) {
    health = {
      ...emptyCampaignProviderHealthSnapshot(),
      healthy: false,
      successRate: 0,
      probes: 3,
      successes: 0,
      minSuccessRate: 0.34,
      providerHealth: {},
      probeOutcomes: [
        {
          error: error instanceof Error ? error.message : String(error)
        }
      ]
    };
  }

  const currentConcurrency = run.telemetry.activeConcurrency ?? run.config.activeConcurrency;
  let nextConcurrency = currentConcurrency;
  let reason = "provider_steady";

  if (!health.healthy && currentConcurrency > 1) {
    nextConcurrency = currentConcurrency - 1;
    reason = "provider_degraded";
  } else if (health.healthy && currentConcurrency < run.config.activeConcurrency) {
    nextConcurrency = currentConcurrency + 1;
    reason = "provider_recovered";
  }

  if (nextConcurrency !== currentConcurrency || !run.telemetry.providerHealth) {
    const queueDepth = await getFieldJobQueueDepth();
    const nextHistory = appendProviderHealthHistory(
      run.telemetry.providerHealthHistory,
      health,
      nextConcurrency,
      queueDepth,
      new Date(now).toISOString()
    );
    await updateCampaignRun({
      id: run.id,
      telemetry: {
        ...run.telemetry,
        providerHealth: health,
        providerHealthHistory: nextHistory,
        activeConcurrency: nextConcurrency,
        lastTuneAt: new Date(now).toISOString()
      }
    });

    const message = nextConcurrency === currentConcurrency
      ? "Refreshed provider health during tuning review"
      : `Adjusted active concurrency from ${currentConcurrency} to ${nextConcurrency}`;

    await appendRuntimeNote(run.id, `${reason}: concurrency ${currentConcurrency} -> ${nextConcurrency}`);
    await appendCampaignRunEvent({
      campaignRunId: run.id,
      eventType: "tuning_action",
      message,
      payload: {
        reason,
        previousConcurrency: currentConcurrency,
        nextConcurrency,
        providerHealth: health as unknown as Record<string, unknown>
      }
    });
  }

  return now;
}

function extractSourceSlugsForRun(run: CampaignRun): string[] {
  if (run.telemetry.cohortManifest && run.telemetry.cohortManifest.length > 0) {
    return [...run.telemetry.cohortManifest];
  }
  if (run.config.frozenSourceSlugs && run.config.frozenSourceSlugs.length > 0) {
    return [...run.config.frozenSourceSlugs];
  }
  const fromEvents = new Set<string>();
  for (const event of run.events) {
    const sourceSlug = (event.payload as Record<string, unknown>).sourceSlug;
    if (typeof sourceSlug === "string" && sourceSlug.trim()) {
      fromEvents.add(sourceSlug.trim());
    }
  }
  if (fromEvents.size > 0) {
    return [...fromEvents];
  }
  return run.items
    .map((item) => `${item.fraternitySlug}-main`)
    .filter(Boolean);
}

function splitTrainEvalSources(sourceSlugs: string[]): { train: string[]; eval: string[] } {
  const midpoint = Math.max(1, Math.ceil(sourceSlugs.length * 0.6));
  return {
    train: sourceSlugs.slice(0, midpoint),
    eval: sourceSlugs.slice(midpoint),
  };
}

async function syncV4LiveTelemetry(run: CampaignRun): Promise<CampaignRun> {
  if (run.config.programMode !== "v4_rl_improvement") {
    return run;
  }
  const sourceSlugs = extractSourceSlugsForRun(run);
  if (sourceSlugs.length === 0) {
    return run;
  }
  const baseline = (run.telemetry.baselineSnapshot ?? null) as V4ProgramSnapshot | null;
  const finalSnapshot = await captureV4ProgramSnapshot(
    sourceSlugs,
    run.config.reviewWindowDays ?? 14,
    run.startedAt ?? run.createdAt
  );
  const thresholdMinutes = run.config.queueStallThresholdMinutes ?? 15;
  const existingAlert = run.telemetry.queueStallAlert ?? {
    active: false,
    since: null,
    reason: null,
    queuedDepth: 0,
    lastProcessedTotal: run.summary.totalProcessed,
  };
  let nextAlert = existingAlert;
  if (run.summary.totalProcessed > existingAlert.lastProcessedTotal) {
    nextAlert = {
      active: false,
      since: null,
      reason: null,
      queuedDepth: finalSnapshot.queueQueued,
      lastProcessedTotal: run.summary.totalProcessed,
    };
  } else if (finalSnapshot.queueQueued > 0) {
    const since = existingAlert.since ?? new Date().toISOString();
    const stalledMs = Date.now() - new Date(since).getTime();
    nextAlert = {
      active: stalledMs >= thresholdMinutes * 60_000,
      since,
      reason: stalledMs >= thresholdMinutes * 60_000 ? "no_meaningful_processed_job_movement" : null,
      queuedDepth: finalSnapshot.queueQueued,
      lastProcessedTotal: existingAlert.lastProcessedTotal,
    };
  }

  const nextTelemetry = {
    ...run.telemetry,
    delayedRewardHealth: {
      delayedRewardEventCount: finalSnapshot.delayedRewardEventCount,
      delayedRewardTotal: finalSnapshot.delayedRewardTotal,
      placeholderReviewCount: finalSnapshot.placeholderReviewCount,
      overlongReviewCount: finalSnapshot.overlongReviewCount,
      guardrailHitRate: finalSnapshot.guardrailHitRate,
      validMissingCount: finalSnapshot.validMissingCount,
      verifiedWebsiteCount: finalSnapshot.verifiedWebsiteCount,
      topDelayedActions: (await getAdaptiveInsights({
        sourceSlugs,
        runtimeMode: run.config.runtimeMode ?? "adaptive_primary",
        windowDays: run.config.reviewWindowDays ?? 14,
        limit: 25,
      })).delayedAttribution.slice(0, 8),
    },
    reviewReasonDrift: baseline ? driftFromSnapshots(baseline, finalSnapshot) : [],
    finalSnapshot: toJsonRecord(finalSnapshot),
    queueStallAlert: nextAlert,
  };
  await updateCampaignRun({
    id: run.id,
    telemetry: nextTelemetry,
  });
  if (nextAlert.active && !existingAlert.active) {
    await appendCampaignRunEvent({
      campaignRunId: run.id,
      eventType: "queue_stall_alert",
      message: "Queue stall detected during the live campaign window",
      payload: nextAlert as unknown as Record<string, unknown>,
    });
  }
  return (await getCampaignRun(run.id)) ?? run;
}

async function runV4Prelude(run: CampaignRun): Promise<CampaignRun> {
  if (run.config.programMode !== "v4_rl_improvement") {
    return run;
  }
  if (run.telemetry.programPhase === "live_campaign" || run.telemetry.programPhase === "completed") {
    return run;
  }

  const effectiveConfig = {
    ...buildDefaultV4ProgramConfig(),
    ...run.config,
    frozenSourceSlugs:
      run.config.frozenSourceSlugs && run.config.frozenSourceSlugs.length > 0
        ? run.config.frozenSourceSlugs
        : buildDefaultV4ProgramConfig().frozenSourceSlugs,
  } as CampaignRunConfig;
  const sourceSlugs = effectiveConfig.frozenSourceSlugs ?? [];
  const baselineSnapshot = await captureV4ProgramSnapshot(sourceSlugs, effectiveConfig.reviewWindowDays ?? 14);
  const initialTelemetry = {
    ...run.telemetry,
    cohortManifest: sourceSlugs,
    baselineSnapshot: toJsonRecord(baselineSnapshot),
    delayedRewardHealth: {
      delayedRewardEventCount: baselineSnapshot.delayedRewardEventCount,
      delayedRewardTotal: baselineSnapshot.delayedRewardTotal,
      placeholderReviewCount: baselineSnapshot.placeholderReviewCount,
      overlongReviewCount: baselineSnapshot.overlongReviewCount,
      guardrailHitRate: baselineSnapshot.guardrailHitRate,
      validMissingCount: baselineSnapshot.validMissingCount,
      verifiedWebsiteCount: baselineSnapshot.verifiedWebsiteCount,
      topDelayedActions: [],
    },
    programPhase: "training" as const,
    programStartedAt: run.telemetry.programStartedAt ?? new Date().toISOString(),
    activePolicyVersion: run.telemetry.activePolicyVersion ?? DEFAULT_POLICY_VERSION,
    activePolicySnapshotId: run.telemetry.activePolicySnapshotId ?? null,
    promotionDecisions: run.telemetry.promotionDecisions ?? [],
  };
  await updateCampaignRun({
    id: run.id,
    config: effectiveConfig,
    telemetry: initialTelemetry,
  });
  await appendCampaignRunEvent({
    campaignRunId: run.id,
    eventType: "v4_baseline_frozen",
    message: "Frozen V4 cohort and captured baseline telemetry",
    payload: {
      sourceSlugs,
      baselineSnapshot,
    },
  });

  const { train, eval: evalSourcesRaw } = splitTrainEvalSources(sourceSlugs);
  const evalSources = evalSourcesRaw.length > 0 ? evalSourcesRaw : sourceSlugs.slice(-Math.max(1, Math.floor(sourceSlugs.length / 3)));
  let bestBalancedScore = Number.NEGATIVE_INFINITY;
  const totalRounds = effectiveConfig.trainingRounds ?? 3;

  for (let round = 1; round <= totalRounds; round += 1) {
    const stagedPolicyVersion = `${DEFAULT_POLICY_VERSION}-${run.id.slice(0, 8)}-r${round}`;
    const roundTrainSources = selectRoundSources(
      train,
      round,
      totalRounds,
      effectiveConfig.trainingSourceBatchSize
    );
    const roundEvalSources = selectRoundSources(
      evalSources,
      round,
      totalRounds,
      effectiveConfig.evalSourceBatchSize
    );
    await appendCampaignRunEvent({
      campaignRunId: run.id,
      eventType: "training_round_started",
      message: `Starting V4 training round ${round}`,
      payload: {
        round,
        stagedPolicyVersion,
        trainSources: roundTrainSources,
        evalSources: roundEvalSources,
        fullTrainSources: train,
        fullEvalSources: evalSources,
        commandTimeoutMinutes: effectiveConfig.trainingCommandTimeoutMinutes ?? 30,
      },
    });
    const trainResult = await runAdaptiveTrainEvalIsolated({
      runId: run.id,
      round,
      trainSourceSlugs: roundTrainSources,
      evalSourceSlugs: roundEvalSources,
      runtimeMode: effectiveConfig.runtimeMode ?? "adaptive_primary",
      policyVersion: stagedPolicyVersion,
      epochsPerRound: effectiveConfig.epochsPerRound ?? 1,
      commandTimeoutMinutes: effectiveConfig.trainingCommandTimeoutMinutes,
      campaignRunId: run.id,
    });
    const latestSnapshot = (
      await listAdaptivePolicySnapshots({
        policyVersion: stagedPolicyVersion,
        runtimeMode: effectiveConfig.runtimeMode ?? "adaptive_primary",
        limit: 1,
      })
    )[0] ?? null;
    const currentSnapshot = await captureV4ProgramSnapshot(sourceSlugs, effectiveConfig.reviewWindowDays ?? 14);
    const kpis = lastRoundKpis(trainResult);
    const promotion = computePromotionReason({
      kpis,
      baseline: baselineSnapshot,
      current: currentSnapshot,
      bestBalancedScore,
    });
    const decision = {
      round,
      stagedPolicyVersion,
      snapshotId: latestSnapshot?.id ?? null,
      promoted: effectiveConfig.checkpointPromotionEnabled ? promotion.promoted : false,
      reason: promotion.reason,
      balancedScore: Number(kpis.balancedScore ?? 0),
      queueQueued: currentSnapshot.queueQueued,
      placeholderReviewCount: currentSnapshot.placeholderReviewCount,
      overlongReviewCount: currentSnapshot.overlongReviewCount,
      createdAt: new Date().toISOString(),
    };
    const nextTelemetry = {
      ...((await getCampaignRun(run.id))?.telemetry ?? initialTelemetry),
      cohortManifest: sourceSlugs,
      baselineSnapshot: toJsonRecord(baselineSnapshot),
      delayedRewardHealth: {
        delayedRewardEventCount: currentSnapshot.delayedRewardEventCount,
        delayedRewardTotal: currentSnapshot.delayedRewardTotal,
        placeholderReviewCount: currentSnapshot.placeholderReviewCount,
        overlongReviewCount: currentSnapshot.overlongReviewCount,
        guardrailHitRate: currentSnapshot.guardrailHitRate,
        validMissingCount: currentSnapshot.validMissingCount,
        verifiedWebsiteCount: currentSnapshot.verifiedWebsiteCount,
        topDelayedActions: [],
      },
      reviewReasonDrift: driftFromSnapshots(baselineSnapshot, currentSnapshot),
      promotionDecisions: [...(((await getCampaignRun(run.id))?.telemetry.promotionDecisions ?? initialTelemetry.promotionDecisions) ?? []), decision],
    };
    if (decision.promoted) {
      bestBalancedScore = Math.max(bestBalancedScore, decision.balancedScore);
      nextTelemetry.activePolicyVersion = stagedPolicyVersion;
      nextTelemetry.activePolicySnapshotId = latestSnapshot?.id ?? null;
    }
    await updateCampaignRun({
      id: run.id,
      telemetry: nextTelemetry,
    });
    await appendCampaignRunEvent({
      campaignRunId: run.id,
      eventType: decision.promoted ? "policy_promoted" : "policy_not_promoted",
      message: decision.promoted
        ? `Promoted staged snapshot from round ${round}`
        : `Did not promote staged snapshot from round ${round}`,
      payload: {
        decision,
        trainResult: {
          policyVersion: trainResult.policy_version,
          reportPath: trainResult.report_path,
          slope: trainResult.slope,
          kpis,
        },
      },
    });
    await appendCampaignRunEvent({
      campaignRunId: run.id,
      eventType: "training_round_completed",
      message: `Completed V4 training round ${round}`,
      payload: {
        round,
        stagedPolicyVersion,
        trainSources: roundTrainSources,
        evalSources: roundEvalSources,
        reportPath: trainResult.report_path,
        balancedScore: Number(kpis.balancedScore ?? 0),
      },
    });
  }

  await updateCampaignRun({
    id: run.id,
    telemetry: {
      ...((await getCampaignRun(run.id))?.telemetry ?? initialTelemetry),
      cohortManifest: sourceSlugs,
      programPhase: "live_campaign",
    },
  });
  return (await getCampaignRun(run.id)) ?? run;
}

async function finalizeCampaign(run: CampaignRun, status: CampaignRun["status"], lastError: string | null): Promise<void> {
  const queueDepthEnd = await getFieldJobQueueDepth();
  const counts = await countCampaignRunItemsByStatus(run.id);
  const queueDepthStart = run.summary.queueDepthStart || queueDepthEnd;
  const summary = buildSummary({
    run,
    queueDepthStart,
    queueDepthEnd,
    counts
  });

  await updateCampaignRun({
    id: run.id,
    status,
    finishedAtNow: true,
    summary,
    telemetry: {
      ...run.telemetry,
      lastCheckpointAt: new Date().toISOString()
    },
    lastError
  });

  await appendCampaignRunEvent({
    campaignRunId: run.id,
    eventType: status === "succeeded" ? "campaign_completed" : status === "canceled" ? "campaign_canceled" : "campaign_failed",
    message: status === "succeeded" ? "Campaign finished" : status === "canceled" ? "Campaign canceled" : "Campaign finished with errors",
    payload: {
      summary,
      lastError
    }
  });

  if (run.config.programMode === "v4_rl_improvement") {
    const refreshed = await getCampaignRun(run.id);
    if (refreshed) {
      const sourceSlugs = extractSourceSlugsForRun(refreshed);
      const finalSnapshot = await captureV4ProgramSnapshot(
        sourceSlugs,
        refreshed.config.reviewWindowDays ?? 14,
        refreshed.startedAt ?? refreshed.createdAt
      );
      const baselineSnapshot = (refreshed.telemetry.baselineSnapshot ?? null) as V4ProgramSnapshot | null;
      const acceptance = buildAcceptanceGate({
        run: refreshed,
        baseline: baselineSnapshot,
        finalSnapshot,
      });
      const nextTelemetry = {
        ...refreshed.telemetry,
        finalSnapshot: toJsonRecord(finalSnapshot),
        acceptanceGate: {
          passed: acceptance.passed,
          checks: acceptance.checks,
          baselineSnapshot: toJsonRecord(baselineSnapshot),
          finalSnapshot: toJsonRecord(finalSnapshot),
        },
        reviewReasonDrift: baselineSnapshot ? driftFromSnapshots(baselineSnapshot, finalSnapshot) : [],
        programPhase: "completed" as const,
      };
      await updateCampaignRun({
        id: refreshed.id,
        telemetry: nextTelemetry,
      });
      await appendCampaignRunEvent({
        campaignRunId: refreshed.id,
        eventType: "acceptance_gate_evaluated",
        message: acceptance.passed ? "V4 acceptance gate passed" : "V4 acceptance gate did not pass",
        payload: {
          checks: acceptance.checks,
        },
      });
      writeV4FinalReport({
        run: (await getCampaignRun(refreshed.id)) ?? refreshed,
        baseline: baselineSnapshot,
        finalSnapshot,
        reportPath: `docs/reports/V4_RL_IMPROVEMENT_${refreshed.id}.md`,
      });
    }
  }
}

export async function runCampaignExecution(runId: string): Promise<void> {
  let run: CampaignRun | null = await getCampaignRun(runId);
  const leaseToken = randomUUID();
  let heartbeat: NodeJS.Timeout | null = null;
  const failSafely = async (message: string) => {
    const latest = run ?? (await getCampaignRun(runId));
    if (!latest) {
      return;
    }
    await finalizeCampaign(latest, "failed", message);
  };

  try {
    await upsertRuntimeWorker({
      workerId: CAMPAIGN_WORKER_ID,
      workloadLane: "campaign",
      runtimeOwner: "evaluation_worker_campaign_runner",
      leaseSeconds: CAMPAIGN_WORKER_LEASE_SECONDS,
      metadata: { runId },
    });

    if (!run) {
      return;
    }
    const claimed = await claimCampaignRunLease({
      runId,
      workerId: CAMPAIGN_WORKER_ID,
      leaseToken,
      leaseSeconds: CAMPAIGN_WORKER_LEASE_SECONDS,
    });
    if (!claimed) {
      await stopRuntimeWorker(CAMPAIGN_WORKER_ID);
      return;
    }

    heartbeat = setInterval(() => {
      void heartbeatRuntimeWorker(CAMPAIGN_WORKER_ID, CAMPAIGN_WORKER_LEASE_SECONDS);
      void heartbeatCampaignRunLease({
        runId,
        workerId: CAMPAIGN_WORKER_ID,
        leaseToken,
        leaseSeconds: CAMPAIGN_WORKER_LEASE_SECONDS,
      });
    }, CAMPAIGN_HEARTBEAT_INTERVAL_MS);

    if (run.status === "canceled") {
      if (heartbeat) {
        clearInterval(heartbeat);
      }
      await releaseCampaignRunLease({ runId, workerId: CAMPAIGN_WORKER_ID, leaseToken });
      return;
    }

    const isResume = Boolean(run.startedAt);

    await reconcileStaleFraternityCrawlRequests();
    await updateCampaignRun({
      id: runId,
      status: "running",
      startedAtNow: !run.startedAt,
      clearFinishedAt: true,
      telemetry: {
        ...run.telemetry,
        activeConcurrency: run.telemetry.activeConcurrency ?? run.config.activeConcurrency
      },
      lastError: null
    });
    await appendCampaignRunEvent({
      campaignRunId: runId,
      eventType: isResume ? "campaign_resumed" : "campaign_started",
      message: isResume ? `Campaign ${run.name} resumed` : `Campaign ${run.name} started`,
      payload: {
        config: run.config
      }
    });

    run = await getCampaignRun(runId);
    if (!run) {
      return;
    }

    if (run.config.preflightRequired) {
    const preflight = await runSearchPreflight(4);
    const startingConcurrency = preflight.healthy ? run.config.activeConcurrency : Math.max(1, run.config.activeConcurrency - 1);
    const queueDepth = await getFieldJobQueueDepth();
    const nextHistory = appendProviderHealthHistory(
      run.telemetry.providerHealthHistory,
      preflight,
      startingConcurrency,
      queueDepth
    );
    await updateCampaignRun({
      id: run.id,
      telemetry: {
        ...run.telemetry,
        providerHealth: preflight,
        providerHealthHistory: nextHistory,
        activeConcurrency: startingConcurrency,
        lastTuneAt: new Date().toISOString()
      }
      });
      await appendCampaignRunEvent({
        campaignRunId: run.id,
        eventType: "preflight_completed",
        message: preflight.healthy ? "Search preflight passed" : "Search preflight is degraded; campaign will run in protected mode",
        payload: preflight as unknown as Record<string, unknown>
      });

      if (preflight.successRate <= 0) {
        const failingRun = await getCampaignRun(run.id);
        if (failingRun) {
          await finalizeCampaign(failingRun, "failed", "Search preflight returned zero successful probes; campaign halted before admission.");
        }
        return;
      }
    }

    run = await runV4Prelude(run);
    run = await ensureCampaignItems(run);
    const queueDepthStart = await getFieldJobQueueDepth();
    await updateCampaignRun({
      id: run.id,
      summary: {
        ...run.summary,
        targetCount: run.config.targetCount,
        itemCount: run.items.length,
        queueDepthStart,
        queueDepthEnd: queueDepthStart,
        queueDepthDelta: 0
      }
    });
    run = (await getCampaignRun(run.id)) ?? run;

    let lastCheckpointAtMs = Date.now();
    let lastTuneAtMs = Date.now();

    while (true) {
      await reconcileStaleFraternityCrawlRequests();
      const latest = await getCampaignRun(run.id);
      if (!latest) {
        return;
      }
      run = await syncCampaignItems(latest);

      if (run.status === "canceled") {
        await finalizeCampaign(run, "canceled", "Campaign canceled by operator.");
        return;
      }

      const counts = await countCampaignRunItemsByStatus(run.id);
      const activeCount = Number(counts.running ?? 0) + Number(counts.queued ?? 0) + Number(counts.request_created ?? 0);
      const concurrency = run.telemetry.activeConcurrency ?? run.config.activeConcurrency;
      const plannedItems = run.items.filter((item) => item.status === "planned");
      const availableSlots = Math.max(0, concurrency - activeCount);

      for (const item of plannedItems.slice(0, availableSlots)) {
        await createRequestForItem(run, item, concurrency);
      }

      run = (await getCampaignRun(run.id)) ?? run;
      run = await refreshRunningSummary(run, queueDepthStart);
      run = await syncV4LiveTelemetry(run);
      lastCheckpointAtMs = await maybeEmitCheckpoint(run, queueDepthStart, lastCheckpointAtMs);
      lastTuneAtMs = await maybeTuneCampaign(run, lastTuneAtMs);
      run = (await getCampaignRun(run.id)) ?? run;

      const terminalCount = run.items.filter((item) => TERMINAL_ITEM_STATUSES.has(item.status)).length;
      const startedAtMs = resolveCampaignStartMs(run);
      const hasTimedOut = startedAtMs
        ? Date.now() - startedAtMs >= run.config.maxDurationMinutes * 60_000
        : false;

      if (terminalCount >= run.items.length && run.items.length > 0) {
        const completedCount = run.items.filter((item) => item.status === "completed").length;
        const failedCount = run.items.filter((item) => item.status === "failed").length;
        const finalStatus = completedCount === 0 && failedCount > 0 ? "failed" : "succeeded";
        const finalError = finalStatus === "failed" ? "All campaign items ended in failure." : null;
        await finalizeCampaign(run, finalStatus, finalError);
        if (heartbeat) {
          clearInterval(heartbeat);
          heartbeat = null;
        }
        await releaseCampaignRunLease({ runId, workerId: CAMPAIGN_WORKER_ID, leaseToken });
        await stopRuntimeWorker(CAMPAIGN_WORKER_ID);
        return;
      }

      if (hasTimedOut) {
        await finalizeCampaign(run, "failed", "Campaign duration exhausted before all items reached a terminal state.");
        if (heartbeat) {
          clearInterval(heartbeat);
          heartbeat = null;
        }
        await releaseCampaignRunLease({ runId, workerId: CAMPAIGN_WORKER_ID, leaseToken });
        await stopRuntimeWorker(CAMPAIGN_WORKER_ID);
        return;
      }

      await delay(run.config.itemPollIntervalMs);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await failSafely(message);
  } finally {
    if (heartbeat) {
      clearInterval(heartbeat);
      heartbeat = null;
    }
    await releaseCampaignRunLease({ runId, workerId: CAMPAIGN_WORKER_ID, leaseToken });
    await stopRuntimeWorker(CAMPAIGN_WORKER_ID);
  }
}

export async function scheduleCampaignRun(runId: string): Promise<boolean> {
  if (activeCampaignRuns.has(runId)) {
    return false;
  }

  activeCampaignRuns.add(runId);
  queueMicrotask(() => {
    void runCampaignExecution(runId).finally(() => {
      activeCampaignRuns.delete(runId);
    });
  });

  return true;
}

export async function scheduleDueCampaignRuns(limit = 10): Promise<number> {
  await reconcileStaleCampaignRuns();
  const dueIds = await listQueuedCampaignRunIds(limit);
  let scheduled = 0;
  for (const id of dueIds) {
    const didSchedule = await scheduleCampaignRun(id);
    if (didSchedule) {
      scheduled += 1;
    }
  }
  return scheduled;
}

export function isCampaignRunActive(runId: string): boolean {
  return activeCampaignRuns.has(runId);
}

export function activeCampaignRunCount(): number {
  return activeCampaignRuns.size;
}

export async function cancelCampaignRun(runId: string): Promise<void> {
  const run = await getCampaignRun(runId);
  if (!run) {
    throw new Error(`Campaign run ${runId} not found`);
  }

  await updateCampaignRun({
    id: runId,
    status: "canceled",
    lastError: "Campaign canceled by operator",
    finishedAtNow: true
  });
  await appendCampaignRunEvent({
    campaignRunId: runId,
    eventType: "campaign_canceled",
    message: "Campaign canceled by operator"
  });

  for (const item of run.items) {
    if (!TERMINAL_ITEM_STATUSES.has(item.status)) {
      await updateCampaignRunItem({
        id: item.id,
        status: "canceled",
        notes: "Campaign canceled by operator"
      });
      if (item.requestId) {
        await updateFraternityCrawlRequest({
          id: item.requestId,
          status: "canceled",
          stage: "failed" as FraternityCrawlRequestStage,
          finishedAtNow: true,
          lastError: "Canceled by campaign operator"
        });
      }
    }
  }
}
