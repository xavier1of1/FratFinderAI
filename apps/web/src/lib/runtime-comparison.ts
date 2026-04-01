import type { CrawlRunListItem } from "@/lib/types";

export type RuntimeFamily = "legacy" | "adaptive";

export interface RuntimeComparisonSummary {
  family: RuntimeFamily;
  runCount: number;
  successRate: number;
  avgRecordsSeen: number;
  avgRecordsUpserted: number;
  avgFieldJobsCreated: number;
  avgReviewItemsCreated: number;
  avgCrawlSessions: number;
  avgPageConfidence: number;
  avgLlmCalls: number;
  avgRecordsPerPage: number;
  latestStartedAt: string | null;
  topStopReasons: Array<{ reason: string; count: number }>;
}

export interface RuntimeComparisonResult {
  scopeLabel: string;
  totalRuns: number;
  legacy: RuntimeComparisonSummary;
  adaptive: RuntimeComparisonSummary;
  deltas: {
    successRate: number;
    avgRecordsSeen: number;
    avgRecordsUpserted: number;
    avgFieldJobsCreated: number;
    avgReviewItemsCreated: number;
    avgRecordsPerPage: number;
    avgCrawlSessions: number;
    avgLlmCalls: number;
  };
}

interface RuntimeComparisonOptions {
  sourceSlug?: string | null;
  sourceSlugs?: string[] | null;
}

const SUCCESS_STATUSES = new Set(["succeeded", "partial"]);

function toRuntimeFamily(runtimeMode: string | null | undefined): RuntimeFamily {
  if (runtimeMode && runtimeMode.toLowerCase().startsWith("adaptive")) {
    return "adaptive";
  }
  return "legacy";
}

function average(total: number, count: number): number {
  if (count <= 0) {
    return 0;
  }
  return total / count;
}

function summarize(family: RuntimeFamily, runs: CrawlRunListItem[]): RuntimeComparisonSummary {
  const runCount = runs.length;
  const successCount = runs.filter((run) => SUCCESS_STATUSES.has(run.status)).length;
  const latestStartedAt = runs
    .map((run) => run.startedAt)
    .sort((left, right) => new Date(right).getTime() - new Date(left).getTime())[0] ?? null;

  const stopReasonCounts = new Map<string, number>();
  for (const run of runs) {
    if (!run.stopReason) {
      continue;
    }
    stopReasonCounts.set(run.stopReason, (stopReasonCounts.get(run.stopReason) ?? 0) + 1);
  }

  const topStopReasons = [...stopReasonCounts.entries()]
    .map(([reason, count]) => ({ reason, count }))
    .sort((left, right) => right.count - left.count)
    .slice(0, 3);

  const totalRecordsSeen = runs.reduce((total, run) => total + run.recordsSeen, 0);
  const totalRecordsUpserted = runs.reduce((total, run) => total + run.recordsUpserted, 0);
  const totalFieldJobs = runs.reduce((total, run) => total + run.fieldJobsCreated, 0);
  const totalReviewItems = runs.reduce((total, run) => total + run.reviewItemsCreated, 0);
  const totalSessions = runs.reduce((total, run) => total + run.crawlSessionCount, 0);
  const totalLlmCalls = runs.reduce((total, run) => total + run.llmCallsUsed, 0);
  const totalRecordsPerPage = runs.reduce((total, run) => total + run.recordsSeen / Math.max(run.pagesProcessed, 1), 0);

  const confidenceValues = runs.map((run) => run.pageLevelConfidence).filter((value): value is number => value !== null);
  const totalConfidence = confidenceValues.reduce((total, value) => total + value, 0);

  return {
    family,
    runCount,
    successRate: average(successCount, runCount),
    avgRecordsSeen: average(totalRecordsSeen, runCount),
    avgRecordsUpserted: average(totalRecordsUpserted, runCount),
    avgFieldJobsCreated: average(totalFieldJobs, runCount),
    avgReviewItemsCreated: average(totalReviewItems, runCount),
    avgCrawlSessions: average(totalSessions, runCount),
    avgPageConfidence: average(totalConfidence, confidenceValues.length),
    avgLlmCalls: average(totalLlmCalls, runCount),
    avgRecordsPerPage: average(totalRecordsPerPage, runCount),
    latestStartedAt,
    topStopReasons
  };
}

export function computeRuntimeComparison(
  runs: CrawlRunListItem[],
  options?: RuntimeComparisonOptions
): RuntimeComparisonResult {
  const providedSlugs = options?.sourceSlugs?.filter((slug): slug is string => Boolean(slug && slug.trim())) ?? [];
  const sourceSlugSet = new Set(providedSlugs);
  if (options?.sourceSlug?.trim()) {
    sourceSlugSet.add(options.sourceSlug.trim());
  }

  const scopedRuns = sourceSlugSet.size
    ? runs.filter((run) => run.sourceSlug && sourceSlugSet.has(run.sourceSlug))
    : runs;

  const legacyRuns = scopedRuns.filter((run) => toRuntimeFamily(run.runtimeMode) === "legacy");
  const adaptiveRuns = scopedRuns.filter((run) => toRuntimeFamily(run.runtimeMode) === "adaptive");

  const legacy = summarize("legacy", legacyRuns);
  const adaptive = summarize("adaptive", adaptiveRuns);

  const scopeLabel = sourceSlugSet.size === 0
    ? "all sources"
    : sourceSlugSet.size === 1
      ? ([...sourceSlugSet][0] ?? "all sources")
      : `${sourceSlugSet.size} campaign sources`;

  return {
    scopeLabel,
    totalRuns: scopedRuns.length,
    legacy,
    adaptive,
    deltas: {
      successRate: adaptive.successRate - legacy.successRate,
      avgRecordsSeen: adaptive.avgRecordsSeen - legacy.avgRecordsSeen,
      avgRecordsUpserted: adaptive.avgRecordsUpserted - legacy.avgRecordsUpserted,
      avgFieldJobsCreated: adaptive.avgFieldJobsCreated - legacy.avgFieldJobsCreated,
      avgReviewItemsCreated: adaptive.avgReviewItemsCreated - legacy.avgReviewItemsCreated,
      avgRecordsPerPage: adaptive.avgRecordsPerPage - legacy.avgRecordsPerPage,
      avgCrawlSessions: adaptive.avgCrawlSessions - legacy.avgCrawlSessions,
      avgLlmCalls: adaptive.avgLlmCalls - legacy.avgLlmCalls
    }
  };
}

