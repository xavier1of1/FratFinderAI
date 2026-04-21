import { NextRequest } from "next/server";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { createEvaluationJob } from "@/lib/repositories/evaluation-job-repository";
import { createBenchmarkRun, failStaleBenchmarkRuns, getBenchmarkRun, listBenchmarkRuns } from "@/lib/repositories/benchmark-repository";
import type { BenchmarkFieldName, BenchmarkRunConfig } from "@/lib/types";

export const dynamic = "force-dynamic";

const DEFAULT_RUNTIME_MODE: "adaptive_shadow" | "adaptive_assisted" = (() => {
  const value = String(process.env.BENCHMARK_CRAWL_RUNTIME_MODE ?? "adaptive_assisted").trim();
  if (value === "adaptive_shadow" || value === "adaptive_assisted") {
    return value;
  }
  return "adaptive_assisted";
})();

const DEFAULT_FIELD_JOB_RUNTIME_MODE: "langgraph_primary" = "langgraph_primary";

const DEFAULT_FIELD_JOB_GRAPH_DURABILITY = (() => {
  const value = String(process.env.BENCHMARK_FIELD_JOB_GRAPH_DURABILITY ?? "sync").trim();
  if (value === "exit" || value === "async" || value === "sync") {
    return value;
  }
  return "sync";
})();
const DEFAULT_WARMUP = (() => {
  const value = String(process.env.BENCHMARK_RUN_ADAPTIVE_WARMUP ?? "true").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
})();


const benchmarkPayloadSchema = z.object({
  name: z.string().trim().min(1).max(120).optional(),
  fieldName: z.enum(["find_website", "find_email", "find_instagram", "all"]).default("find_email"),
  sourceSlug: z.string().trim().min(1).max(160).optional().nullable(),
  workers: z.coerce.number().int().min(1).max(16).default(8),
  limitPerCycle: z.coerce.number().int().min(1).max(500).default(25),
  cycles: z.coerce.number().int().min(1).max(100).default(6),
  pauseMs: z.coerce.number().int().min(0).max(10_000).default(500),
  crawlRuntimeMode: z.enum(["adaptive_shadow", "adaptive_assisted"]).default(DEFAULT_RUNTIME_MODE),
  fieldJobRuntimeMode: z.enum(["langgraph_primary"]).default(DEFAULT_FIELD_JOB_RUNTIME_MODE),
  fieldJobGraphDurability: z.enum(["exit", "async", "sync"]).default(DEFAULT_FIELD_JOB_GRAPH_DURABILITY),
  runAdaptiveCrawlBeforeCycles: z.coerce.boolean().default(DEFAULT_WARMUP),
  isolationMode: z.enum(["shared_live_observed", "strict_live_isolated"]).default("shared_live_observed")
});

function formatDefaultBenchmarkName(fieldName: BenchmarkFieldName): string {
  const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19);
  return `${fieldName} benchmark ${timestamp}`;
}

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "100");

    const data = await listBenchmarkRuns(Number.isNaN(limit) ? 100 : Math.min(Math.max(limit, 1), 500));
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export async function POST(request: NextRequest) {
  try {
    await failStaleBenchmarkRuns();
    const body = await request.json();
    const payload = benchmarkPayloadSchema.parse(body);

    const fieldName = payload.fieldName;
    const sourceSlug = payload.sourceSlug?.trim() ? payload.sourceSlug.trim() : null;

    const config: BenchmarkRunConfig = {
      fieldName,
      sourceSlug,
      workers: payload.workers,
      limitPerCycle: payload.limitPerCycle,
      cycles: payload.cycles,
      pauseMs: payload.pauseMs,
      crawlRuntimeMode: payload.crawlRuntimeMode,
      fieldJobRuntimeMode: payload.fieldJobRuntimeMode,
      fieldJobGraphDurability: payload.fieldJobGraphDurability,
      runAdaptiveCrawlBeforeCycles: payload.runAdaptiveCrawlBeforeCycles,
      isolationMode: payload.isolationMode
    };

    const created = await createBenchmarkRun({
      name: payload.name?.trim() || formatDefaultBenchmarkName(fieldName),
      fieldName,
      sourceSlug,
      config
    });

    await createEvaluationJob({
      jobKind: "benchmark_run",
      benchmarkRunId: created.id,
      sourceSlug,
      isolationMode: payload.isolationMode,
      payload: {
        fieldName,
        sourceSlug,
        isolationMode: payload.isolationMode,
      },
    });

    const latest = await getBenchmarkRun(created.id);
    return apiSuccess(latest ?? created, { status: 202 });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
