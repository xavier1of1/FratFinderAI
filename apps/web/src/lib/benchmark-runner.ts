import { spawn } from "child_process";
import { existsSync } from "fs";
import path from "path";

import {
  completeBenchmarkRun,
  failBenchmarkRun,
  getBenchmarkRun,
  getFieldJobStatusSnapshot,
  markBenchmarkRunStarted
} from "@/lib/repositories/benchmark-repository";
import type {
  BenchmarkCycleSample,
  BenchmarkFieldName,
  BenchmarkQueueSnapshot,
  BenchmarkRunConfig,
  BenchmarkRunSummary
} from "@/lib/types";

const activeRuns = new Set<string>();

interface ProcessFieldJobResult {
  processed: number;
  requeued: number;
  failedTerminal: number;
}

interface BenchmarkTotals {
  processed: number;
  requeued: number;
  failedTerminal: number;
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
  const processed = readLastNumericValue(output, "processed");
  const requeued = readLastNumericValue(output, "requeued");
  const failedTerminal = readLastNumericValue(output, "failed_terminal");

  if (processed === null || requeued === null || failedTerminal === null) {
    throw new Error(`Could not parse process-field-jobs output: ${output.slice(-600)}`);
  }

  return { processed, requeued, failedTerminal };
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

  if (config.fieldName !== "all") {
    args.push("--field-name", config.fieldName);
  }

  if (config.sourceSlug) {
    args.push("--source-slug", config.sourceSlug);
  }

  const timeoutMs = Math.max(30_000, Math.min(180_000, config.limitPerCycle * 2_500));
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
}): BenchmarkRunSummary {
  const elapsedMs = Math.max(Date.now() - params.startedAtMs, 1);
  const jobsPerMinute = (params.totals.processed * 60_000) / elapsedMs;
  const avgCycleMs = params.cyclesCompleted > 0 ? elapsedMs / params.cyclesCompleted : 0;

  return {
    elapsedMs,
    cyclesCompleted: params.cyclesCompleted,
    totalProcessed: params.totals.processed,
    totalRequeued: params.totals.requeued,
    totalFailedTerminal: params.totals.failedTerminal,
    jobsPerMinute,
    avgCycleMs,
    queueDepthStart: params.startSnapshot.queued,
    queueDepthEnd: params.endSnapshot.queued,
    queueDepthDelta: params.startSnapshot.queued - params.endSnapshot.queued
  };
}

async function executeBenchmarkRun(runId: string): Promise<void> {
  const startedAtMs = Date.now();
  const samples: BenchmarkCycleSample[] = [];
  const totals: BenchmarkTotals = {
    processed: 0,
    requeued: 0,
    failedTerminal: 0
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

    for (let cycle = 1; cycle <= config.cycles; cycle += 1) {
      const cycleStartedAt = new Date().toISOString();
      const cycleStartedAtMs = Date.now();
      const result = await runFieldJobCycle(config);

      totals.processed += result.processed;
      totals.requeued += result.requeued;
      totals.failedTerminal += result.failedTerminal;

      endSnapshot = await getFieldJobStatusSnapshot({
        fieldName: config.fieldName,
        sourceSlug: config.sourceSlug
      });

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
        failed: endSnapshot.failed
      });

      if (config.pauseMs > 0 && cycle < config.cycles) {
        await delay(config.pauseMs);
      }
    }

    const summary = buildSummary({
      startedAtMs,
      totals,
      cyclesCompleted: samples.length,
      startSnapshot,
      endSnapshot
    });

    await completeBenchmarkRun({
      id: runId,
      summary,
      samples
    });
  } catch (error) {
    try {
      const summary = buildSummary({
        startedAtMs,
        totals,
        cyclesCompleted: samples.length,
        startSnapshot,
        endSnapshot
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
  }
}

export async function scheduleBenchmarkRun(runId: string): Promise<boolean> {
  if (activeRuns.has(runId)) {
    return false;
  }

  activeRuns.add(runId);
  queueMicrotask(() => {
    void executeBenchmarkRun(runId).finally(() => {
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
