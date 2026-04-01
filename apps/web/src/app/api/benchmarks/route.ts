import { NextRequest } from "next/server";
import { z } from "zod";

import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleBenchmarkRun } from "@/lib/benchmark-runner";
import { createBenchmarkRun, failStaleBenchmarkRuns, getBenchmarkRun, listBenchmarkRuns } from "@/lib/repositories/benchmark-repository";
import type { BenchmarkFieldName, BenchmarkRunConfig } from "@/lib/types";


const DEFAULT_RUNTIME_MODE = (() => {
  const value = String(process.env.BENCHMARK_CRAWL_RUNTIME_MODE ?? "adaptive_assisted").trim();
  if (value === "legacy" || value === "adaptive_shadow" || value === "adaptive_assisted" || value === "adaptive_primary") {
    return value;
  }
  return "adaptive_assisted";
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
  crawlRuntimeMode: z.enum(["legacy", "adaptive_shadow", "adaptive_assisted", "adaptive_primary"]).default(DEFAULT_RUNTIME_MODE),
  runAdaptiveCrawlBeforeCycles: z.coerce.boolean().default(DEFAULT_WARMUP)
});

function formatDefaultBenchmarkName(fieldName: BenchmarkFieldName): string {
  const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19);
  return `${fieldName} benchmark ${timestamp}`;
}

export async function GET(request: NextRequest) {
  try {
    await failStaleBenchmarkRuns();
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
      runAdaptiveCrawlBeforeCycles: payload.runAdaptiveCrawlBeforeCycles
    };

    const created = await createBenchmarkRun({
      name: payload.name?.trim() || formatDefaultBenchmarkName(fieldName),
      fieldName,
      sourceSlug,
      config
    });

    const scheduled = await scheduleBenchmarkRun(created.id);
    if (!scheduled) {
      return apiError({
        status: 409,
        code: "benchmark_already_running",
        message: `Benchmark ${created.id} is already running.`
      });
    }

    const latest = await getBenchmarkRun(created.id);
    return apiSuccess(latest ?? created, { status: 202 });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
