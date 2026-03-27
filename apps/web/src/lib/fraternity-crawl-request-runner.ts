import { spawn } from "child_process";
import { existsSync } from "fs";
import path from "path";

import {
  appendFraternityCrawlRequestEvent,
  getFraternityCrawlRequest,
  getLatestCrawlRunForSource,
  getSourceFieldJobSnapshot,
  listDueQueuedFraternityCrawlRequestIds,
  reconcileStaleFraternityCrawlRequests,
  updateFraternityCrawlRequest
} from "@/lib/repositories/fraternity-crawl-request-repository";
import type { FraternityCrawlProgress } from "@/lib/types";

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

function parseFieldJobResult(output: string): { processed: number; requeued: number; failedTerminal: number } {
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
    failedTerminal: readLast("failed_terminal")
  };
}

function buildProgressSnapshot(params: {
  sourceUrl: string | null;
  sourceConfidence: number | null;
  confidenceTier: string | null;
  candidates: unknown[];
  crawlRun: Awaited<ReturnType<typeof getLatestCrawlRunForSource>>;
  fieldSnapshot: Awaited<ReturnType<typeof getSourceFieldJobSnapshot>>;
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
    totals
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

  await updateFraternityCrawlRequest({
    id: requestId,
    status: "running",
    stage: "crawl_run",
    startedAtNow: true,
    clearFinishedAt: true,
    lastError: null
  });
  await appendFraternityCrawlRequestEvent({
    requestId,
    eventType: "stage_started",
    message: "Crawl run stage started",
    payload: { stage: "crawl_run", sourceSlug: request.sourceSlug }
  });

  try {
    await runPythonCommand(["-m", "fratfinder_crawler.cli", "run", "--source-slug", request.sourceSlug], 15 * 60_000);

    const crawlRunAfterIngest = await getLatestCrawlRunForSource(request.sourceSlug);
    const fieldSnapshotAfterIngest = await getSourceFieldJobSnapshot(request.sourceSlug);
    const progressAfterIngest = buildProgressSnapshot({
      sourceUrl: request.sourceUrl,
      sourceConfidence: request.sourceConfidence,
      confidenceTier: request.sourceConfidence !== null ? (request.sourceConfidence >= 0.8 ? "high" : request.sourceConfidence >= 0.6 ? "medium" : "low") : "low",
      candidates: request.progress.discovery?.candidates ?? [],
      crawlRun: crawlRunAfterIngest,
      fieldSnapshot: fieldSnapshotAfterIngest
    });

    await updateFraternityCrawlRequest({
      id: requestId,
      progress: progressAfterIngest
    });

    if ((progressAfterIngest.crawlRun?.recordsSeen ?? 0) <= 0) {
      await updateFraternityCrawlRequest({
        id: requestId,
        status: "failed",
        stage: "failed",
        finishedAtNow: true,
        progress: progressAfterIngest,
        lastError: "No chapters discovered from the selected national source. Confirm source URL or parser strategy."
      });
      await appendFraternityCrawlRequestEvent({
        requestId,
        eventType: "stage_failed",
        message: "Crawl run discovered zero chapters; request halted before enrichment",
        payload: {
          stage: "crawl_run",
          crawlRun: progressAfterIngest.crawlRun
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

    const config = request.config;
    const maxCycles = Math.max(1, config.maxEnrichmentCycles);

    for (let cycle = 1; cycle <= maxCycles; cycle += 1) {
      const commandResult = await runPythonCommand(
        [
          "-m",
          "fratfinder_crawler.cli",
          "process-field-jobs",
          "--source-slug",
          request.sourceSlug,
          "--workers",
          String(config.fieldJobWorkers),
          "--limit",
          String(config.fieldJobLimitPerCycle)
        ],
        10 * 60_000
      );

      const parsed = parseFieldJobResult(`${commandResult.stdout}\n${commandResult.stderr}`);
      const latestRun = await getLatestCrawlRunForSource(request.sourceSlug);
      const fieldSnapshot = await getSourceFieldJobSnapshot(request.sourceSlug);

      const progress = buildProgressSnapshot({
        sourceUrl: request.sourceUrl,
        sourceConfidence: request.sourceConfidence,
        confidenceTier: request.sourceConfidence !== null ? (request.sourceConfidence >= 0.8 ? "high" : request.sourceConfidence >= 0.6 ? "medium" : "low") : "low",
        candidates: request.progress.discovery?.candidates ?? [],
        crawlRun: latestRun,
        fieldSnapshot
      });

      await updateFraternityCrawlRequest({
        id: requestId,
        progress
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
          totals: progress.totals
        }
      });

      const totals = progress.totals ?? { queued: 0, running: 0 };
      if ((totals.queued ?? 0) + (totals.running ?? 0) === 0) {
        await updateFraternityCrawlRequest({
          id: requestId,
          status: "succeeded",
          stage: "completed",
          finishedAtNow: true,
          progress
        });
        await appendFraternityCrawlRequestEvent({
          requestId,
          eventType: "request_completed",
          message: "Fraternity crawl request completed",
          payload: { totals: progress.totals }
        });
        return;
      }

      if (cycle < maxCycles && config.pauseMs > 0) {
        await delay(config.pauseMs);
      }
    }

    const latestRun = await getLatestCrawlRunForSource(request.sourceSlug);
    const fieldSnapshot = await getSourceFieldJobSnapshot(request.sourceSlug);
    const progress = buildProgressSnapshot({
      sourceUrl: request.sourceUrl,
      sourceConfidence: request.sourceConfidence,
      confidenceTier: request.sourceConfidence !== null ? (request.sourceConfidence >= 0.8 ? "high" : request.sourceConfidence >= 0.6 ? "medium" : "low") : "low",
      candidates: request.progress.discovery?.candidates ?? [],
      crawlRun: latestRun,
      fieldSnapshot
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
