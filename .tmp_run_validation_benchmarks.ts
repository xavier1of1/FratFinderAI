import { spawn } from "child_process";
import {
  createBenchmarkRun,
  completeBenchmarkRun,
  failBenchmarkRun,
  getFieldJobStatusSnapshot,
  markBenchmarkRunStarted,
} from "./apps/web/src/lib/repositories/benchmark-repository";

const SOURCES = [
  "alpha-delta-phi-main",
  "beta-upsilon-chi-main",
  "sigma-chi-main",
  "alpha-gamma-rho-main",
  "delta-chi-main",
];

type CycleResult = { processed: number; requeued: number; failedTerminal: number };

type Totals = { processed: number; requeued: number; failedTerminal: number };

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function readLastNumericValue(output: string, key: string): number | null {
  const pattern = new RegExp(`"${key}"\\s*:\\s*(-?\\d+)`, "g");
  let match: RegExpExecArray | null = null;
  let last: number | null = null;
  while (true) {
    match = pattern.exec(output);
    if (!match) break;
    last = Number(match[1]);
  }
  return last;
}

function runCycle(sourceSlug: string, workers: number, limitPerCycle: number): Promise<CycleResult> {
  const args = [
    "-m",
    "fratfinder_crawler.cli",
    "process-field-jobs",
    "--source-slug",
    sourceSlug,
    "--workers",
    String(workers),
    "--limit",
    String(limitPerCycle),
  ];

  return new Promise((resolve, reject) => {
    const child = spawn("python", args, {
      cwd: process.cwd(),
      env: process.env,
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf-8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf-8");
    });

    child.on("error", (error) => reject(error));

    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`process-field-jobs exited ${code}: ${stderr || stdout}`));
        return;
      }
      const processed = readLastNumericValue(`${stdout}\n${stderr}`, "processed");
      const requeued = readLastNumericValue(`${stdout}\n${stderr}`, "requeued");
      const failedTerminal = readLastNumericValue(`${stdout}\n${stderr}`, "failed_terminal");
      if (processed === null || requeued === null || failedTerminal === null) {
        reject(new Error(`Could not parse process-field-jobs output: ${stdout}\n${stderr}`));
        return;
      }
      resolve({ processed, requeued, failedTerminal });
    });
  });
}

async function runBenchmarkForSource(sourceSlug: string): Promise<void> {
  const config = {
    fieldName: "all" as const,
    sourceSlug,
    workers: 8,
    limitPerCycle: 80,
    cycles: 6,
    pauseMs: 400,
  };

  const run = await createBenchmarkRun({
    name: `Validation benchmark ${sourceSlug} ${new Date().toISOString()}`,
    fieldName: "all",
    sourceSlug,
    config,
  });

  console.log(JSON.stringify({ createdRunId: run.id, sourceSlug }, null, 2));

  const startedAtMs = Date.now();
  const samples: Array<{
    cycle: number;
    startedAt: string;
    durationMs: number;
    processed: number;
    requeued: number;
    failedTerminal: number;
    queued: number;
    running: number;
    done: number;
    failed: number;
  }> = [];
  const totals: Totals = { processed: 0, requeued: 0, failedTerminal: 0 };

  const startSnapshot = await getFieldJobStatusSnapshot({ fieldName: "all", sourceSlug });
  let endSnapshot = startSnapshot;

  try {
    await markBenchmarkRunStarted(run.id);

    for (let cycle = 1; cycle <= config.cycles; cycle += 1) {
      const cycleStartedAtMs = Date.now();
      const cycleStartedAt = new Date().toISOString();
      const result = await runCycle(sourceSlug, config.workers, config.limitPerCycle);
      totals.processed += result.processed;
      totals.requeued += result.requeued;
      totals.failedTerminal += result.failedTerminal;

      endSnapshot = await getFieldJobStatusSnapshot({ fieldName: "all", sourceSlug });
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
      });

      if (cycle < config.cycles && config.pauseMs > 0) {
        await sleep(config.pauseMs);
      }
    }

    const elapsedMs = Math.max(Date.now() - startedAtMs, 1);
    const summary = {
      elapsedMs,
      cyclesCompleted: samples.length,
      totalProcessed: totals.processed,
      totalRequeued: totals.requeued,
      totalFailedTerminal: totals.failedTerminal,
      jobsPerMinute: (totals.processed * 60000) / elapsedMs,
      avgCycleMs: samples.length > 0 ? elapsedMs / samples.length : 0,
      queueDepthStart: startSnapshot.queued,
      queueDepthEnd: endSnapshot.queued,
      queueDepthDelta: startSnapshot.queued - endSnapshot.queued,
    };

    await completeBenchmarkRun({ id: run.id, summary, samples });
    console.log(JSON.stringify({ completedRunId: run.id, sourceSlug, summary }, null, 2));
  } catch (error) {
    const elapsedMs = Math.max(Date.now() - startedAtMs, 1);
    const summary = {
      elapsedMs,
      cyclesCompleted: samples.length,
      totalProcessed: totals.processed,
      totalRequeued: totals.requeued,
      totalFailedTerminal: totals.failedTerminal,
      jobsPerMinute: (totals.processed * 60000) / elapsedMs,
      avgCycleMs: samples.length > 0 ? elapsedMs / samples.length : 0,
      queueDepthStart: startSnapshot.queued,
      queueDepthEnd: endSnapshot.queued,
      queueDepthDelta: startSnapshot.queued - endSnapshot.queued,
    };
    await failBenchmarkRun({
      id: run.id,
      error: error instanceof Error ? error.message : String(error),
      summary,
      samples,
    });
    throw error;
  }
}

async function main(): Promise<void> {
  for (const sourceSlug of SOURCES) {
    await runBenchmarkForSource(sourceSlug);
  }
}

main()
  .then(() => {
    console.log("All validation benchmarks finished");
    process.exit(0);
  })
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
