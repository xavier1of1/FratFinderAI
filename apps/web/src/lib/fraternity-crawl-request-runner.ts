import { spawn } from "child_process";
import { existsSync } from "fs";
import path from "path";

import { discoverFraternitySource } from "@/lib/fraternity-discovery";
import { evaluateSourceUrl, pickBestDiscoveryCandidate } from "@/lib/source-selection";
import {
  appendFraternityCrawlRequestEvent,
  getFraternityCrawlRequest,
  getLatestCrawlRunForSource,
  getSourceFieldJobSnapshot,
  listDueQueuedFraternityCrawlRequestIds,
  reconcileStaleFraternityCrawlRequests,
  updateFraternityCrawlRequest,
  upsertFraternityRecord,
  upsertSourceRecord
} from "@/lib/repositories/fraternity-crawl-request-repository";
import type {
  FraternityCrawlEnrichmentAnalytics,
  FraternityCrawlProgress,
  FraternityCrawlRequest,
  FraternityCrawlSourceQuality
} from "@/lib/types";

const DEFAULT_FIELD_JOB_RUNTIME_MODE = (() => {
  const value = String(process.env.FIELD_JOB_RUNTIME_MODE ?? process.env["Agent:FIELD_JOB_RUNTIME_MODE"] ?? "langgraph_primary").trim();
  if (value === "legacy" || value === "langgraph_shadow" || value === "langgraph_primary") {
    return value;
  }
  return "langgraph_primary";
})();

const DEFAULT_FIELD_JOB_GRAPH_DURABILITY = (() => {
  const value = String(process.env.FIELD_JOB_GRAPH_DURABILITY ?? process.env["Agent:FIELD_JOB_GRAPH_DURABILITY"] ?? "async").trim();
  if (value === "exit" || value === "async" || value === "sync") {
    return value;
  }
  return "async";
})();
const activeRequestRuns = new Set<string>();

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

interface CommandResult {
  stdout: string;
  stderr: string;
}

function computeFieldJobCycleTimeoutMs(workers: number, limitPerCycle: number): number {
  const scaledTimeout = limitPerCycle * Math.max(workers, 1) * 4_000;
  return Math.max(10 * 60_000, Math.min(30 * 60_000, scaledTimeout));
}

function computeCrawlRunTimeoutMs(): number {
  return 30 * 60_000;
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
      settle(
        (value) => reject(value as Error),
        error
      );
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

      settle(
        (value) => resolve(value as CommandResult),
        { stdout, stderr }
      );
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
function parseFieldJobResult(output: string): {
  processed: number;
  requeued: number;
  failedTerminal: number;
  runtimeFallbackCount: number;
  runtimeModeUsed: string | null;
} {
  try {
    const payload = parseTrailingJson<Record<string, unknown>>(output);
    return {
      processed: Number(payload.processed ?? 0),
      requeued: Number(payload.requeued ?? 0),
      failedTerminal: Number(payload.failed_terminal ?? payload.failedTerminal ?? 0),
      runtimeFallbackCount: Number(payload.runtime_fallback_count ?? payload.runtimeFallbackCount ?? 0),
      runtimeModeUsed:
        typeof payload.runtime_mode_used === "string"
          ? payload.runtime_mode_used
          : typeof payload.runtimeModeUsed === "string"
            ? payload.runtimeModeUsed
            : null
    };
  } catch {
    const readLast = (key: string) => {
      const pattern = new RegExp(`"${key}"\\s*:\\s*(-?\\d+)`, "g");
      let match: RegExpExecArray | null = null;
      let value: number | null = null;
      while (true) {
        match = pattern.exec(output);
        if (!match) {
          break;
        }
        value = Number(match[1]);
      }
      if (value === null) {
        throw new Error(`Could not parse ${key} from process-field-jobs output`);
      }
      return value;
    };

    return {
      processed: readLast("processed"),
      requeued: readLast("requeued"),
      failedTerminal: readLast("failed_terminal"),
      runtimeFallbackCount: 0,
      runtimeModeUsed: null
    };
  }
}

function cloneProgress(progress: FraternityCrawlProgress | undefined): FraternityCrawlProgress {
  return progress ? JSON.parse(JSON.stringify(progress)) as FraternityCrawlProgress : {};
}

function totalFieldJobs(totals: Record<string, number> | undefined): number {
  if (!totals) {
    return 0;
  }
  return Number(totals.queued ?? 0) + Number(totals.running ?? 0) + Number(totals.done ?? 0) + Number(totals.failed ?? 0);
}

function computeAdaptiveEnrichmentConfig(
  baseConfig: FraternityCrawlRequest["config"],
  progress: FraternityCrawlProgress,
  cycleState: {
    cyclesCompleted: number;
    lowProgressCycles: number;
    degradedCycleCount: number;
  }
): FraternityCrawlRequest["config"] {
  const discovered = Number(progress.crawlRun?.recordsSeen ?? 0);
  const queueSize = totalFieldJobs(progress.totals);
  const baseWorkers = Math.max(1, Number(baseConfig.fieldJobWorkers ?? 1));
  const baseLimit = Math.max(1, Number(baseConfig.fieldJobLimitPerCycle ?? 1));
  const queuePressure = Math.max(queueSize, discovered);

  let effectiveWorkers = baseWorkers;
  let effectiveLimit = baseLimit;
  let adaptiveMaxCycles = Math.max(1, Number(baseConfig.maxEnrichmentCycles ?? 1));
  let budgetStrategy = "base";

  if (queuePressure >= 300) {
    effectiveWorkers = Math.max(effectiveWorkers, 10);
    effectiveLimit = Math.max(effectiveLimit, 100);
    adaptiveMaxCycles = Math.max(adaptiveMaxCycles, 72);
    budgetStrategy = "high_volume";
  } else if (queuePressure >= 150) {
    effectiveWorkers = Math.max(effectiveWorkers, 8);
    effectiveLimit = Math.max(effectiveLimit, 80);
    adaptiveMaxCycles = Math.max(adaptiveMaxCycles, 48);
    budgetStrategy = "medium_volume";
  } else if (queuePressure >= 60) {
    effectiveWorkers = Math.max(effectiveWorkers, 6);
    effectiveLimit = Math.max(effectiveLimit, 60);
    adaptiveMaxCycles = Math.max(adaptiveMaxCycles, 32);
    budgetStrategy = "moderate_volume";
  }

  if (cycleState.lowProgressCycles >= 2) {
    effectiveWorkers = Math.max(1, effectiveWorkers - 1);
    effectiveLimit = Math.max(20, Math.floor(effectiveLimit * 0.8));
    adaptiveMaxCycles = Math.min(96, adaptiveMaxCycles + 6);
    budgetStrategy = `${budgetStrategy}_stabilized`;
  }

  if (cycleState.degradedCycleCount >= 2) {
    effectiveWorkers = Math.max(1, Math.min(effectiveWorkers, 4));
    effectiveLimit = Math.max(20, Math.min(effectiveLimit, 50));
    adaptiveMaxCycles = Math.min(96, adaptiveMaxCycles + 4);
    budgetStrategy = `${budgetStrategy}_degraded`;
  }

  return {
    fieldJobWorkers: effectiveWorkers,
    fieldJobLimitPerCycle: effectiveLimit,
    maxEnrichmentCycles: Math.min(96, adaptiveMaxCycles),
    pauseMs: baseConfig.pauseMs
  };
}

function updateProgressAnalytics(
  progress: FraternityCrawlProgress,
  analytics: {
    sourceQuality?: FraternityCrawlSourceQuality;
    enrichment?: FraternityCrawlEnrichmentAnalytics;
  }
): FraternityCrawlProgress {
  const next = cloneProgress(progress);
  next.analytics = next.analytics ?? {};
  if (analytics.sourceQuality) {
    next.analytics.sourceQuality = analytics.sourceQuality;
  }
  if (analytics.enrichment) {
    next.analytics.enrichment = analytics.enrichment;
  }
  return next;
}

function buildProgressSnapshot(params: {
  sourceUrl: string | null;
  sourceConfidence: number | null;
  confidenceTier: string | null;
  sourceProvenance: "verified_registry" | "existing_source" | "search" | null;
  fallbackReason: string | null;
  resolutionTrace: Array<Record<string, unknown>>;
  candidates: unknown[];
  crawlRun: Awaited<ReturnType<typeof getLatestCrawlRunForSource>>;
  fieldSnapshot: Awaited<ReturnType<typeof getSourceFieldJobSnapshot>>;
  analytics?: FraternityCrawlProgress["analytics"];
}): FraternityCrawlProgress {
  const fields = {
    find_website: { queued: 0, running: 0, done: 0, failed: 0 },
    find_email: { queued: 0, running: 0, done: 0, failed: 0 },
    find_instagram: { queued: 0, running: 0, done: 0, failed: 0 }
  };

  for (const item of params.fieldSnapshot) {
    fields[item.field] = {
      queued: item.queued,
      running: item.running,
      done: item.done,
      failed: item.failed
    };
  }

  const totals = {
    queued: fields.find_website.queued + fields.find_email.queued + fields.find_instagram.queued,
    running: fields.find_website.running + fields.find_email.running + fields.find_instagram.running,
    done: fields.find_website.done + fields.find_email.done + fields.find_instagram.done,
    failed: fields.find_website.failed + fields.find_email.failed + fields.find_instagram.failed
  };

  return {
    discovery: {
      sourceUrl: params.sourceUrl,
      sourceConfidence: params.sourceConfidence ?? 0,
      confidenceTier: params.confidenceTier ?? "low",
      sourceProvenance: params.sourceProvenance,
      fallbackReason: params.fallbackReason,
      resolutionTrace: params.resolutionTrace,
      candidates: params.candidates as never
    },
    crawlRun: params.crawlRun
      ? {
          id: params.crawlRun.id,
          status: params.crawlRun.status,
          pagesProcessed: params.crawlRun.pagesProcessed,
          recordsSeen: params.crawlRun.recordsSeen,
          recordsUpserted: params.crawlRun.recordsUpserted,
          reviewItemsCreated: params.crawlRun.reviewItemsCreated,
          fieldJobsCreated: params.crawlRun.fieldJobsCreated
        }
      : {
          id: null,
          status: null,
          pagesProcessed: 0,
          recordsSeen: 0,
          recordsUpserted: 0,
          reviewItemsCreated: 0,
          fieldJobsCreated: 0
    },
    fields,
    totals,
    analytics: params.analytics
  };
}

async function executeFraternityCrawlRequest(requestId: string): Promise<void> {
  const request = await getFraternityCrawlRequest(requestId);
  if (!request) {
    return;
  }
  if (request.status !== "queued" && request.status !== "running") {
    return;
  }
  if (!request.sourceSlug) {
    await updateFraternityCrawlRequest({
      id: requestId,
      status: "failed",
      stage: "failed",
      finishedAtNow: true,
      lastError: "Missing source slug for crawl execution"
    });
    await appendFraternityCrawlRequestEvent({
      requestId,
      eventType: "request_failed",
      message: "Request failed because source slug is missing"
    });
    return;
  }

  const confidenceTier =
    request.sourceConfidence !== null ? (request.sourceConfidence >= 0.8 ? "high" : request.sourceConfidence >= 0.6 ? "medium" : "low") : "low";
  let currentSourceQuality = request.progress.analytics?.sourceQuality ?? {
    ...evaluateSourceUrl(request.sourceUrl),
    recoveryAttempts: Number(request.progress.analytics?.sourceQuality?.recoveryAttempts ?? 0),
    recoveredFromUrl: request.progress.analytics?.sourceQuality?.recoveredFromUrl ?? null,
    recoveredToUrl: request.progress.analytics?.sourceQuality?.recoveredToUrl ?? null,
    sourceRejectedCount: Number(request.progress.analytics?.sourceQuality?.sourceRejectedCount ?? 0),
    sourceRecoveredCount: Number(request.progress.analytics?.sourceQuality?.sourceRecoveredCount ?? 0),
    zeroChapterPrevented: Number(request.progress.analytics?.sourceQuality?.zeroChapterPrevented ?? 0)
  };

  if (currentSourceQuality.isWeak) {
    const recoveryAttempts = Number(currentSourceQuality.recoveryAttempts ?? 0);
    if (recoveryAttempts < 1) {
      try {
        const rediscovered = await discoverFraternitySource(request.fraternityName);
        const alternateCandidate = pickBestDiscoveryCandidate(rediscovered.candidates, request.sourceUrl);
        const alternateUrl = alternateCandidate?.url ?? rediscovered.selectedUrl;
        const normalizedCurrent = (request.sourceUrl ?? "").replace(/\/+$/, "");
        const normalizedAlternate = (alternateUrl ?? "").replace(/\/+$/, "");
        if (alternateUrl && normalizedAlternate !== normalizedCurrent) {
          const alternateQuality = evaluateSourceUrl(alternateUrl);
          if (!alternateQuality.isWeak && alternateQuality.score > currentSourceQuality.score + 0.08) {
            const nextSourceSlug = `${rediscovered.fraternitySlug || request.fraternitySlug}-main`;
            const nextSource = new URL(alternateUrl);
            const recoveredFraternity = await upsertFraternityRecord({
              slug: rediscovered.fraternitySlug || request.fraternitySlug,
              name: rediscovered.fraternityName || request.fraternityName,
              nicAffiliated: true
            });
            await upsertSourceRecord({
              fraternityId: recoveredFraternity.id,
              slug: nextSourceSlug,
              baseUrl: nextSource.origin,
              listPath: alternateUrl,
              sourceType: "html_directory",
              parserKey: "directory_v1",
              active: true,
              metadata: {
                discovery: {
                  selectedUrl: alternateUrl,
                  selectedConfidence: rediscovered.selectedConfidence,
                  confidenceTier: rediscovered.confidenceTier,
                  sourceProvenance: rediscovered.sourceProvenance,
                  fallbackReason: rediscovered.fallbackReason,
                  resolutionTrace: rediscovered.resolutionTrace,
                  sourceQuality: alternateQuality,
                  selectedCandidateRationale: rediscovered.selectedCandidateRationale
                }
              }
            });

            currentSourceQuality = {
              ...alternateQuality,
              recoveryAttempts: recoveryAttempts + 1,
              recoveredFromUrl: request.sourceUrl,
              recoveredToUrl: alternateUrl,
              sourceRejectedCount: Number(currentSourceQuality.sourceRejectedCount ?? 0),
              sourceRecoveredCount: Number(currentSourceQuality.sourceRecoveredCount ?? 0) + 1,
              zeroChapterPrevented: Number(currentSourceQuality.zeroChapterPrevented ?? 0) + 1
            };

            const recoveredProgress = updateProgressAnalytics(
              {
                ...request.progress,
                discovery: {
                  sourceUrl: alternateUrl,
                  sourceConfidence: rediscovered.selectedConfidence,
                  confidenceTier: rediscovered.confidenceTier,
                  sourceProvenance: rediscovered.sourceProvenance,
                  fallbackReason: rediscovered.fallbackReason,
                  sourceQuality: alternateQuality,
                  selectedCandidateRationale: rediscovered.selectedCandidateRationale,
                  resolutionTrace: rediscovered.resolutionTrace,
                  candidates: rediscovered.candidates
                }
              },
              {
                sourceQuality: currentSourceQuality
              }
            );

            await updateFraternityCrawlRequest({
              id: requestId,
              sourceSlug: nextSourceSlug,
              sourceUrl: alternateUrl,
              sourceConfidence: rediscovered.selectedConfidence,
              progress: recoveredProgress,
              lastError: null
            });
            await appendFraternityCrawlRequestEvent({
              requestId,
              eventType: "source_recovered",
              message: "Recovered weak source before crawl stage and switched to a stronger candidate",
              payload: {
                previousSourceUrl: request.sourceUrl,
                nextSourceUrl: alternateUrl,
                previousQuality: currentSourceQuality,
                nextQuality: alternateQuality,
                fallbackReason: rediscovered.fallbackReason,
                rationale: rediscovered.selectedCandidateRationale
              }
            });
            await executeFraternityCrawlRequest(requestId);
            return;
          }
        }
      } catch {
        // Fall through to awaiting_confirmation with source diagnostics.
      }
    }

    const rejectedSourceQuality = {
      ...currentSourceQuality,
      sourceRejectedCount: Number(currentSourceQuality.sourceRejectedCount ?? 0) + 1
    };
    await updateFraternityCrawlRequest({
      id: requestId,
      status: "draft",
      stage: "awaiting_confirmation",
      clearFinishedAt: true,
      lastError: `Source requires confirmation before crawl (${rejectedSourceQuality.reasons.join(", ") || "insufficient_source_quality"}).`,
      progress: updateProgressAnalytics(request.progress, {
        sourceQuality: rejectedSourceQuality
      })
    });
    await appendFraternityCrawlRequestEvent({
      requestId,
      eventType: "source_rejected",
      message: "Request moved to awaiting_confirmation because source quality is weak",
      payload: {
        sourceUrl: request.sourceUrl,
        sourceQuality: rejectedSourceQuality,
        alternatives: request.progress.discovery?.candidates?.slice(0, 5) ?? []
      }
    });
    return;
  }

  await updateFraternityCrawlRequest({
    id: requestId,
    status: "running",
    stage: "crawl_run",
    startedAtNow: true,
    clearFinishedAt: true,
    lastError: null,
    progress: updateProgressAnalytics(request.progress, {
      sourceQuality: currentSourceQuality
    })
  });
  await appendFraternityCrawlRequestEvent({
    requestId,
    eventType: "stage_started",
    message: "Crawl run stage started",
    payload: { stage: "crawl_run", sourceSlug: request.sourceSlug }
  });

  const crawlStageStartedAt = new Date().toISOString();
  const crawlRunBaseline = await getLatestCrawlRunForSource(request.sourceSlug);
  const crawlRunQueryOptions = {
    startedAfter: crawlStageStartedAt,
    excludeRunId: crawlRunBaseline?.id ?? null
  };

  try {
    try {
      await runPythonCommand(["-m", "fratfinder_crawler.cli", "run", "--source-slug", request.sourceSlug], computeCrawlRunTimeoutMs());
    } catch (error) {
      const crawlRunAfterTimeout = await getLatestCrawlRunForSource(request.sourceSlug, crawlRunQueryOptions);
      const fieldSnapshotAfterTimeout = await getSourceFieldJobSnapshot(request.sourceSlug);
      const progressAfterTimeout = buildProgressSnapshot({
        sourceUrl: request.sourceUrl,
        sourceConfidence: request.sourceConfidence,
        confidenceTier,
        sourceProvenance: request.progress.discovery?.sourceProvenance ?? null,
        fallbackReason: request.progress.discovery?.fallbackReason ?? null,
        resolutionTrace: request.progress.discovery?.resolutionTrace ?? [],
        candidates: request.progress.discovery?.candidates ?? [],
        crawlRun: crawlRunAfterTimeout,
        fieldSnapshot: fieldSnapshotAfterTimeout,
        analytics: {
          sourceQuality: currentSourceQuality,
          enrichment: request.progress.analytics?.enrichment
        }
      });
      const message = error instanceof Error ? error.message : String(error);
      const crawlWorkDetected =
        (progressAfterTimeout.crawlRun?.recordsSeen ?? 0) > 0 ||
        (progressAfterTimeout.crawlRun?.fieldJobsCreated ?? 0) > 0;

      await updateFraternityCrawlRequest({
        id: requestId,
        progress: progressAfterTimeout
      });

      if (message.includes("timed out") && crawlWorkDetected) {
        await appendFraternityCrawlRequestEvent({
          requestId,
          eventType: "stage_degraded",
          message: "Crawl run timed out after producing usable work; continuing into enrichment",
          payload: {
            stage: "crawl_run",
            error: message,
            crawlRun: progressAfterTimeout.crawlRun,
            totals: progressAfterTimeout.totals
          }
        });
      } else {
        throw error;
      }
    }

    const crawlRunAfterIngest = await getLatestCrawlRunForSource(request.sourceSlug, crawlRunQueryOptions);
    const fieldSnapshotAfterIngest = await getSourceFieldJobSnapshot(request.sourceSlug);
    let progressAfterIngest = buildProgressSnapshot({
      sourceUrl: request.sourceUrl,
      sourceConfidence: request.sourceConfidence,
      confidenceTier,
      sourceProvenance: request.progress.discovery?.sourceProvenance ?? null,
      fallbackReason: request.progress.discovery?.fallbackReason ?? null,
      resolutionTrace: request.progress.discovery?.resolutionTrace ?? [],
      candidates: request.progress.discovery?.candidates ?? [],
      crawlRun: crawlRunAfterIngest,
      fieldSnapshot: fieldSnapshotAfterIngest,
      analytics: {
        sourceQuality: currentSourceQuality,
        enrichment: request.progress.analytics?.enrichment
      }
    });

    await updateFraternityCrawlRequest({
      id: requestId,
      progress: progressAfterIngest
    });

    if (!progressAfterIngest.crawlRun?.id) {
      await updateFraternityCrawlRequest({
        id: requestId,
        status: "failed",
        stage: "failed",
        finishedAtNow: true,
        progress: progressAfterIngest,
        lastError: "Crawl command completed but no new crawl run was recorded for this request"
      });
      await appendFraternityCrawlRequestEvent({
        requestId,
        eventType: "stage_failed",
        message: "Crawl run stage did not create a new crawl run record",
        payload: {
          stage: "crawl_run",
          crawlRunBaseline: crawlRunBaseline?.id ?? null,
          crawlStageStartedAt
        }
      });
      return;
    }

    if ((progressAfterIngest.crawlRun?.recordsSeen ?? 0) <= 0) {
      const recoveryAttempts = Number(currentSourceQuality.recoveryAttempts ?? 0);
      if (recoveryAttempts < 1) {
        try {
          const rediscovered = await discoverFraternitySource(request.fraternityName);
          const alternateCandidate = pickBestDiscoveryCandidate(rediscovered.candidates, request.sourceUrl);
          const alternateUrl = alternateCandidate?.url ?? rediscovered.selectedUrl;
          if (alternateUrl && alternateUrl.replace(/\/+$/, "") !== (request.sourceUrl ?? "").replace(/\/+$/, "")) {
            const alternateQuality = evaluateSourceUrl(alternateUrl);
            if (alternateQuality.score > currentSourceQuality.score + 0.12) {
              const nextSourceSlug = `${rediscovered.fraternitySlug || request.fraternitySlug}-main`;
              const nextSource = new URL(alternateUrl);
              const recoveredFraternity = await upsertFraternityRecord({
                slug: rediscovered.fraternitySlug || request.fraternitySlug,
                name: rediscovered.fraternityName || request.fraternityName,
                nicAffiliated: true
              });
              await upsertSourceRecord({
                fraternityId: recoveredFraternity.id,
                slug: nextSourceSlug,
                baseUrl: nextSource.origin,
                listPath: alternateUrl,
                sourceType: "html_directory",
                parserKey: "directory_v1",
                active: true,
                metadata: {
                  discovery: {
                    selectedUrl: alternateUrl,
                    selectedConfidence: rediscovered.selectedConfidence,
                    confidenceTier: rediscovered.confidenceTier,
                    sourceProvenance: rediscovered.sourceProvenance,
                    fallbackReason: rediscovered.fallbackReason,
                    resolutionTrace: rediscovered.resolutionTrace,
                    sourceQuality: alternateQuality
                  }
                }
              });

              const recoveredProgress = updateProgressAnalytics(
                {
                  ...progressAfterIngest,
                  discovery: {
                    sourceUrl: alternateUrl,
                    sourceConfidence: rediscovered.selectedConfidence,
                    confidenceTier: rediscovered.confidenceTier,
                    sourceProvenance: rediscovered.sourceProvenance,
                    fallbackReason: rediscovered.fallbackReason,
                    resolutionTrace: rediscovered.resolutionTrace,
                    candidates: rediscovered.candidates
                  }
                },
                {
                  sourceQuality: {
                    ...alternateQuality,
                    recoveryAttempts: recoveryAttempts + 1,
                    recoveredFromUrl: request.sourceUrl,
                    recoveredToUrl: alternateUrl
                  }
                }
              );

              await updateFraternityCrawlRequest({
                id: requestId,
                sourceSlug: nextSourceSlug,
                sourceUrl: alternateUrl,
                sourceConfidence: rediscovered.selectedConfidence,
                progress: recoveredProgress,
                lastError: null
              });
              await appendFraternityCrawlRequestEvent({
                requestId,
                eventType: "source_recovered",
                message: "Recovered from a zero-chapter national source by switching to a stronger candidate",
                payload: {
                  previousSourceUrl: request.sourceUrl,
                  nextSourceUrl: alternateUrl,
                  previousQuality: currentSourceQuality,
                  nextQuality: alternateQuality,
                  fallbackReason: rediscovered.fallbackReason
                }
              });
              await appendFraternityCrawlRequestEvent({
                requestId,
                eventType: "stage_restarted",
                message: "Restarting crawl run after source recovery",
                payload: {
                  stage: "crawl_run",
                  sourceSlug: nextSourceSlug
                }
              });
              await executeFraternityCrawlRequest(requestId);
              return;
            }
          }
        } catch {
          // Fall through to terminal failure with the original diagnostics.
        }
      }

      const zeroChapterQuality = {
        ...currentSourceQuality,
        sourceRejectedCount: Number(currentSourceQuality.sourceRejectedCount ?? 0) + 1,
        zeroChapterPrevented: Number(currentSourceQuality.zeroChapterPrevented ?? 0) + 1
      };
      await updateFraternityCrawlRequest({
        id: requestId,
        status: "draft",
        stage: "awaiting_confirmation",
        clearFinishedAt: true,
        progress: updateProgressAnalytics(progressAfterIngest, {
          sourceQuality: zeroChapterQuality
        }),
        lastError: "No chapters discovered from the selected national source. Source held for confirmation before rerun."
      });
      await appendFraternityCrawlRequestEvent({
        requestId,
        eventType: "source_rejected",
        message: "Crawl run discovered zero chapters; request moved to awaiting_confirmation",
        payload: {
          stage: "crawl_run",
          crawlRun: progressAfterIngest.crawlRun,
          sourceQuality: zeroChapterQuality
        }
      });
      return;
    }

    await appendFraternityCrawlRequestEvent({
      requestId,
      eventType: "stage_completed",
      message: "Crawl run stage completed",
      payload: { stage: "crawl_run" }
    });

    await updateFraternityCrawlRequest({
      id: requestId,
      stage: "enrichment"
    });

    let cycleState = {
      cyclesCompleted: 0,
      lowProgressCycles: 0,
      degradedCycleCount: 0
    };
    let effectiveConfig = computeAdaptiveEnrichmentConfig(request.config, progressAfterIngest, cycleState);
    progressAfterIngest = updateProgressAnalytics(progressAfterIngest, {
      sourceQuality: currentSourceQuality,
      enrichment: {
        adaptiveMaxEnrichmentCycles: effectiveConfig.maxEnrichmentCycles,
        effectiveFieldJobWorkers: effectiveConfig.fieldJobWorkers,
        effectiveFieldJobLimitPerCycle: effectiveConfig.fieldJobLimitPerCycle,
        cyclesCompleted: cycleState.cyclesCompleted,
        lowProgressCycles: cycleState.lowProgressCycles,
        degradedCycleCount: cycleState.degradedCycleCount,
        queueAtStart: totalFieldJobs(progressAfterIngest.totals),
        queueRemaining: totalFieldJobs(progressAfterIngest.totals),
        runtimeFallbackCount: Number(progressAfterIngest.analytics?.enrichment?.runtimeFallbackCount ?? 0),
        queueBurnRate: Number(progressAfterIngest.analytics?.enrichment?.queueBurnRate ?? 0),
        budgetStrategy: "initial_adaptive_budget"
      }
    });
    await updateFraternityCrawlRequest({
      id: requestId,
      progress: progressAfterIngest
    });

    for (let cycle = 1; cycle <= effectiveConfig.maxEnrichmentCycles; cycle += 1) {
      let commandResult: CommandResult | null = null;
      let runtimeModeUsed: "legacy" | "langgraph_shadow" | "langgraph_primary" = DEFAULT_FIELD_JOB_RUNTIME_MODE as "legacy" | "langgraph_shadow" | "langgraph_primary";
      let runtimeFallbackCount = 0;
      const cycleTimeoutMs = computeFieldJobCycleTimeoutMs(effectiveConfig.fieldJobWorkers, effectiveConfig.fieldJobLimitPerCycle);
      const buildJobArgs = (mode: "legacy" | "langgraph_shadow" | "langgraph_primary") => {
        const args = [
          "-m",
          "fratfinder_crawler.cli",
          "process-field-jobs",
          "--source-slug",
          request.sourceSlug as string,
          "--workers",
          String(effectiveConfig.fieldJobWorkers),
          "--limit",
          String(effectiveConfig.fieldJobLimitPerCycle),
          "--run-preflight",
          "--runtime-mode",
          mode
        ];
        if (mode !== "legacy") {
          args.push("--graph-durability", DEFAULT_FIELD_JOB_GRAPH_DURABILITY);
        }
        return args;
      };

      try {
        commandResult = await runPythonCommand(buildJobArgs(runtimeModeUsed), cycleTimeoutMs);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const shouldFallbackToLegacy = runtimeModeUsed !== "legacy" && !message.includes("timed out");

        if (shouldFallbackToLegacy) {
          try {
            commandResult = await runPythonCommand(buildJobArgs("legacy"), cycleTimeoutMs);
            runtimeModeUsed = "legacy";
            runtimeFallbackCount = 1;
            await appendFraternityCrawlRequestEvent({
              requestId,
              eventType: "runtime_fallback",
              message: `Enrichment cycle ${cycle} fell back to legacy runtime`,
              payload: {
                cycle,
                fromRuntime: DEFAULT_FIELD_JOB_RUNTIME_MODE,
                toRuntime: "legacy",
                error: message
              }
            });
          } catch {
            commandResult = null;
          }
        }

        if (!commandResult) {
          const latestRun = await getLatestCrawlRunForSource(request.sourceSlug);
          const fieldSnapshot = await getSourceFieldJobSnapshot(request.sourceSlug);
          const progress = buildProgressSnapshot({
            sourceUrl: request.sourceUrl,
            sourceConfidence: request.sourceConfidence,
            confidenceTier,
            sourceProvenance: request.progress.discovery?.sourceProvenance ?? null,
            fallbackReason: request.progress.discovery?.fallbackReason ?? null,
            resolutionTrace: request.progress.discovery?.resolutionTrace ?? [],
            candidates: request.progress.discovery?.candidates ?? [],
            crawlRun: latestRun,
            fieldSnapshot,
            analytics: request.progress.analytics
          });
          const totals = progress.totals ?? { queued: 0, running: 0, done: 0, failed: 0 };
          const remainingWork = (totals.queued ?? 0) + (totals.running ?? 0);
          const isTimeout = message.includes("timed out");

          await updateFraternityCrawlRequest({
            id: requestId,
            progress
          });

          await appendFraternityCrawlRequestEvent({
            requestId,
            eventType: isTimeout ? "enrichment_cycle_timeout" : "enrichment_cycle_error",
            message: isTimeout ? `Enrichment cycle ${cycle} timed out` : `Enrichment cycle ${cycle} failed`,
            payload: {
              cycle,
              error: message,
              totals,
              runtimeMode: runtimeModeUsed,
              runtimeFallbackCount,
              adaptiveConfig: effectiveConfig
            }
          });

          if (isTimeout && remainingWork > 0 && cycle < effectiveConfig.maxEnrichmentCycles) {
            cycleState.degradedCycleCount += 1;
            effectiveConfig = computeAdaptiveEnrichmentConfig(request.config, progress, cycleState);
            if (effectiveConfig.pauseMs > 0) {
              await delay(effectiveConfig.pauseMs);
            }
            continue;
          }

          throw error;
        }
      }

      const parsed = parseFieldJobResult(`${commandResult.stdout}\n${commandResult.stderr}`);
      const latestRun = await getLatestCrawlRunForSource(request.sourceSlug);
      const fieldSnapshot = await getSourceFieldJobSnapshot(request.sourceSlug);

      cycleState.cyclesCompleted = cycle;
      const progress = buildProgressSnapshot({
        sourceUrl: request.sourceUrl,
        sourceConfidence: request.sourceConfidence,
        confidenceTier,
        sourceProvenance: request.progress.discovery?.sourceProvenance ?? null,
        fallbackReason: request.progress.discovery?.fallbackReason ?? null,
        resolutionTrace: request.progress.discovery?.resolutionTrace ?? [],
        candidates: request.progress.discovery?.candidates ?? [],
        crawlRun: latestRun,
        fieldSnapshot
      });
      const remainingQueue = Number(progress.totals?.queued ?? 0) + Number(progress.totals?.running ?? 0);
      const lowSignalCycle = parsed.processed <= 0 && parsed.requeued > 0;
      cycleState.lowProgressCycles = lowSignalCycle ? cycleState.lowProgressCycles + 1 : 0;
      if (parsed.requeued > Math.max(parsed.processed * 3, 20)) {
        cycleState.degradedCycleCount += 1;
      }
      const nextProgress = updateProgressAnalytics(progress, {
        sourceQuality: currentSourceQuality,
        enrichment: {
          adaptiveMaxEnrichmentCycles: effectiveConfig.maxEnrichmentCycles,
          effectiveFieldJobWorkers: effectiveConfig.fieldJobWorkers,
          effectiveFieldJobLimitPerCycle: effectiveConfig.fieldJobLimitPerCycle,
          cyclesCompleted: cycleState.cyclesCompleted,
          lowProgressCycles: cycleState.lowProgressCycles,
          degradedCycleCount: cycleState.degradedCycleCount,
          queueAtStart: Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals)),
          queueRemaining: remainingQueue,
          runtimeFallbackCount:
            Number(progressAfterIngest.analytics?.enrichment?.runtimeFallbackCount ?? 0) +
            Number(parsed.runtimeFallbackCount ?? 0) +
            runtimeFallbackCount,
          queueBurnRate:
            Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals)) > 0
              ? Number(
                  (
                    (Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals)) -
                      remainingQueue) /
                    Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals))
                  ).toFixed(4)
                )
              : 0,
          budgetStrategy:
            cycleState.degradedCycleCount > 0 ? "adaptive_degraded" : cycleState.lowProgressCycles > 0 ? "adaptive_stabilized" : "adaptive_steady"
        }
      });
      effectiveConfig = computeAdaptiveEnrichmentConfig(request.config, nextProgress, cycleState);

      await updateFraternityCrawlRequest({
        id: requestId,
        progress: nextProgress
      });

      await appendFraternityCrawlRequestEvent({
        requestId,
        eventType: "enrichment_cycle",
        message: `Enrichment cycle ${cycle} completed`,
        payload: {
          cycle,
          processed: parsed.processed,
          requeued: parsed.requeued,
          failedTerminal: parsed.failedTerminal,
          runtimeModeUsed: parsed.runtimeModeUsed ?? runtimeModeUsed,
          runtimeFallbackCount: Number(parsed.runtimeFallbackCount ?? 0) + runtimeFallbackCount,
          totals: nextProgress.totals,
          adaptiveConfig: effectiveConfig
        }
      });

      const totals = nextProgress.totals ?? { queued: 0, running: 0 };
      if ((totals.queued ?? 0) + (totals.running ?? 0) === 0) {
        await updateFraternityCrawlRequest({
          id: requestId,
          status: "succeeded",
          stage: "completed",
          finishedAtNow: true,
          progress: nextProgress
        });
        await appendFraternityCrawlRequestEvent({
          requestId,
          eventType: "request_completed",
          message: "Fraternity crawl request completed",
          payload: { totals: nextProgress.totals }
        });
        return;
      }

      if (cycle < effectiveConfig.maxEnrichmentCycles && effectiveConfig.pauseMs > 0) {
        await delay(effectiveConfig.pauseMs);
      }
    }

    const latestRun = await getLatestCrawlRunForSource(request.sourceSlug);
    const fieldSnapshot = await getSourceFieldJobSnapshot(request.sourceSlug);
    const exhaustedQueueRemaining =
      fieldSnapshot.reduce((sum, item) => sum + item.queued + item.running, 0);
    const progress = buildProgressSnapshot({
      sourceUrl: request.sourceUrl,
      sourceConfidence: request.sourceConfidence,
      confidenceTier,
      sourceProvenance: request.progress.discovery?.sourceProvenance ?? null,
      fallbackReason: request.progress.discovery?.fallbackReason ?? null,
      resolutionTrace: request.progress.discovery?.resolutionTrace ?? [],
      candidates: request.progress.discovery?.candidates ?? [],
      crawlRun: latestRun,
      fieldSnapshot,
      analytics: updateProgressAnalytics(request.progress, {
        sourceQuality: currentSourceQuality,
        enrichment: {
          adaptiveMaxEnrichmentCycles: effectiveConfig.maxEnrichmentCycles,
          effectiveFieldJobWorkers: effectiveConfig.fieldJobWorkers,
          effectiveFieldJobLimitPerCycle: effectiveConfig.fieldJobLimitPerCycle,
          cyclesCompleted: cycleState.cyclesCompleted,
          lowProgressCycles: cycleState.lowProgressCycles,
          degradedCycleCount: cycleState.degradedCycleCount,
          queueAtStart: Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals)),
          queueRemaining: exhaustedQueueRemaining,
          runtimeFallbackCount: Number(progressAfterIngest.analytics?.enrichment?.runtimeFallbackCount ?? 0),
          queueBurnRate:
            Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals)) > 0
              ? Number(
                  ((Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals)) - exhaustedQueueRemaining) /
                    Number(progressAfterIngest.analytics?.enrichment?.queueAtStart ?? totalFieldJobs(progressAfterIngest.totals))).toFixed(4)
                )
              : 0,
          budgetStrategy: "adaptive_budget_exhausted"
        }
      }).analytics
    });

    await updateFraternityCrawlRequest({
      id: requestId,
      status: "failed",
      stage: "failed",
      finishedAtNow: true,
      progress,
      lastError: "Enrichment cycle budget exhausted before queue drained"
    });
    await appendFraternityCrawlRequestEvent({
      requestId,
      eventType: "request_failed",
      message: "Request failed: enrichment cycle budget exhausted",
      payload: { totals: progress.totals }
    });
  } catch (error) {
    await updateFraternityCrawlRequest({
      id: requestId,
      status: "failed",
      stage: "failed",
      finishedAtNow: true,
      lastError: error instanceof Error ? error.message : String(error)
    });
    await appendFraternityCrawlRequestEvent({
      requestId,
      eventType: "request_failed",
      message: "Fraternity crawl request failed",
      payload: { error: error instanceof Error ? error.message : String(error) }
    });
  }
}

export async function scheduleFraternityCrawlRequest(requestId: string): Promise<boolean> {
  if (activeRequestRuns.has(requestId)) {
    return false;
  }

  activeRequestRuns.add(requestId);
  queueMicrotask(() => {
    void executeFraternityCrawlRequest(requestId).finally(() => {
      activeRequestRuns.delete(requestId);
    });
  });

  return true;
}

export async function scheduleDueFraternityCrawlRequests(limit = 20): Promise<number> {
  await reconcileStaleFraternityCrawlRequests();
  const dueIds = await listDueQueuedFraternityCrawlRequestIds(limit);
  let scheduled = 0;
  for (const id of dueIds) {
    const didSchedule = await scheduleFraternityCrawlRequest(id);
    if (didSchedule) {
      scheduled += 1;
    }
  }
  return scheduled;
}

export function isFraternityCrawlRequestActive(requestId: string): boolean {
  return activeRequestRuns.has(requestId);
}

export function activeFraternityCrawlRequestCount(): number {
  return activeRequestRuns.size;
}
