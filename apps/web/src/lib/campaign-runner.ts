import { spawn } from "child_process";
import { existsSync } from "fs";
import path from "path";

import { discoverFraternitySource } from "@/lib/fraternity-discovery";
import { scheduleFraternityCrawlRequest } from "@/lib/fraternity-crawl-request-runner";
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
  getFieldJobQueueDepth,
  getPreferredCampaignSourceForFraternity,
  getSourceCoverageSnapshot,
  insertCampaignItems,
  listQueuedCampaignRunIds,
  reconcileStaleCampaignRuns,
  selectCampaignFraternities,
  updateCampaignRun,
  updateCampaignRunItem
} from "@/lib/repositories/campaign-run-repository";
import type {
  CampaignProviderHealthHistoryPoint,
  CampaignProviderHealthSnapshot,
  CampaignRun,
  CampaignRunConfig,
  CampaignRunItem,
  CampaignRunSummary,
  CampaignScorecard,
  FraternityCrawlRequest,
  FraternityCrawlRequestConfig,
  FraternityCrawlRequestStage,
  FraternityCrawlRequestStatus
} from "@/lib/types";

const activeCampaignRuns = new Set<string>();
const TERMINAL_ITEM_STATUSES = new Set(["completed", "failed", "skipped", "canceled"]);

interface CommandResult {
  stdout: string;
  stderr: string;
}

interface SearchPreflightResult extends CampaignProviderHealthSnapshot {
  probeOutcomes?: Array<Record<string, unknown>>;
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

function buildDefaultRequestConfig(campaignConfig: CampaignRunConfig, concurrency: number): FraternityCrawlRequestConfig {
  const workers = Math.max(3, Math.min(12, concurrency <= 1 ? 5 : 8));
  const limitPerCycle = Math.max(30, Math.min(120, concurrency <= 1 ? 40 : 60));
  return {
    fieldJobWorkers: workers,
    fieldJobLimitPerCycle: limitPerCycle,
    maxEnrichmentCycles: Math.max(18, Math.min(60, Math.round(campaignConfig.maxDurationMinutes / 3.5))),
    pauseMs: 750
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
  return {
    targetCount: params.run.config.targetCount,
    itemCount: params.run.items.length,
    completedCount: Number(params.counts.completed ?? 0),
    failedCount: Number(params.counts.failed ?? 0),
    skippedCount: Number(params.counts.skipped ?? 0),
    activeCount: Number(params.counts.running ?? 0) + Number(params.counts.queued ?? 0) + Number(params.counts.request_created ?? 0),
    anyContactSuccessRate: chaptersWithAnyContact / denominator,
    allThreeSuccessRate: chaptersWithAllThree / denominator,
    websiteCoverageRate: websitesFound / denominator,
    emailCoverageRate: emailsFound / denominator,
    instagramCoverageRate: instagramsFound / denominator,
    jobsPerMinute: (totalProcessed * 60_000) / durationMs,
    queueDepthStart: params.queueDepthStart,
    queueDepthEnd: params.queueDepthEnd,
    queueDepthDelta: params.queueDepthStart - params.queueDepthEnd,
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
  const config = buildDefaultRequestConfig(run.config, concurrency);
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
      sourceQuality: selectedQuality
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
      selectionReason: selectedSource.selectionReason
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
}

async function executeCampaignRun(runId: string): Promise<void> {
  let run: CampaignRun | null = await getCampaignRun(runId);
  const failSafely = async (message: string) => {
    const latest = run ?? (await getCampaignRun(runId));
    if (!latest) {
      return;
    }
    await finalizeCampaign(latest, "failed", message);
  };

  try {
    if (!run) {
      return;
    }
    if (run.status === "canceled") {
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
        return;
      }

      if (hasTimedOut) {
        await finalizeCampaign(run, "failed", "Campaign duration exhausted before all items reached a terminal state.");
        return;
      }

      await delay(run.config.itemPollIntervalMs);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await failSafely(message);
  }
}

export async function scheduleCampaignRun(runId: string): Promise<boolean> {
  if (activeCampaignRuns.has(runId)) {
    return false;
  }

  activeCampaignRuns.add(runId);
  queueMicrotask(() => {
    void executeCampaignRun(runId).finally(() => {
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
