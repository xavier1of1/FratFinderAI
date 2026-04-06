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

export interface ChapterSearchComparisonSummary {
  family: RuntimeFamily;
  runCount: number;
  avgCanonicalCreated: number;
  avgProvisionalCreated: number;
  avgRejected: number;
  avgNationalFollowed: number;
  avgInstitutionalFollowed: number;
  avgChapterOwnedSkipped: number;
  avgBroaderWebFollowed: number;
  avgWallTimeMs: number;
}

export interface ChapterSearchComparisonResult {
  scopeLabel: string;
  totalRuns: number;
  legacy: ChapterSearchComparisonSummary;
  adaptive: ChapterSearchComparisonSummary;
  deltas: {
    avgCanonicalCreated: number;
    avgProvisionalCreated: number;
    avgRejected: number;
    avgNationalFollowed: number;
    avgInstitutionalFollowed: number;
    avgChapterOwnedSkipped: number;
    avgBroaderWebFollowed: number;
    avgWallTimeMs: number;
  };
  gates: Array<{
    label: string;
    value: string;
    target: string;
    passed: boolean;
  }>;
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

function summarizeChapterSearch(family: RuntimeFamily, runs: CrawlRunListItem[]): ChapterSearchComparisonSummary {
  const chapterSearchRuns = runs.filter((run) => run.chapterSearch);
  const runCount = chapterSearchRuns.length;

  const totals = chapterSearchRuns.reduce(
    (accumulator, run) => {
      const chapterSearch = run.chapterSearch ?? {};
      accumulator.canonical += chapterSearch.canonicalChaptersCreated ?? 0;
      accumulator.provisional += chapterSearch.provisionalChaptersCreated ?? 0;
      accumulator.rejected += chapterSearch.candidatesRejected ?? 0;
      accumulator.national += chapterSearch.nationalTargetsFollowed ?? 0;
      accumulator.institutional += chapterSearch.institutionalTargetsFollowed ?? 0;
      accumulator.skipped += chapterSearch.chapterOwnedTargetsSkipped ?? 0;
      accumulator.broader += chapterSearch.broaderWebTargetsFollowed ?? 0;
      accumulator.wallTime += chapterSearch.chapterSearchWallTimeMs ?? 0;
      return accumulator;
    },
    {
      canonical: 0,
      provisional: 0,
      rejected: 0,
      national: 0,
      institutional: 0,
      skipped: 0,
      broader: 0,
      wallTime: 0,
    }
  );

  return {
    family,
    runCount,
    avgCanonicalCreated: average(totals.canonical, runCount),
    avgProvisionalCreated: average(totals.provisional, runCount),
    avgRejected: average(totals.rejected, runCount),
    avgNationalFollowed: average(totals.national, runCount),
    avgInstitutionalFollowed: average(totals.institutional, runCount),
    avgChapterOwnedSkipped: average(totals.skipped, runCount),
    avgBroaderWebFollowed: average(totals.broader, runCount),
    avgWallTimeMs: average(totals.wallTime, runCount),
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

export function computeChapterSearchComparison(
  runs: CrawlRunListItem[],
  options?: RuntimeComparisonOptions
): ChapterSearchComparisonResult {
  const providedSlugs = options?.sourceSlugs?.filter((slug): slug is string => Boolean(slug && slug.trim())) ?? [];
  const sourceSlugSet = new Set(providedSlugs);
  if (options?.sourceSlug?.trim()) {
    sourceSlugSet.add(options.sourceSlug.trim());
  }

  const scopedRuns = sourceSlugSet.size
    ? runs.filter((run) => run.sourceSlug && sourceSlugSet.has(run.sourceSlug))
    : runs;

  const chapterSearchRuns = scopedRuns.filter((run) => run.chapterSearch);
  const legacyRuns = chapterSearchRuns.filter((run) => toRuntimeFamily(run.runtimeMode) === "legacy");
  const adaptiveRuns = chapterSearchRuns.filter((run) => toRuntimeFamily(run.runtimeMode) === "adaptive");
  const legacy = summarizeChapterSearch("legacy", legacyRuns);
  const adaptive = summarizeChapterSearch("adaptive", adaptiveRuns);

  const scopeLabel = sourceSlugSet.size === 0
    ? "all sources"
    : sourceSlugSet.size === 1
      ? ([...sourceSlugSet][0] ?? "all sources")
      : `${sourceSlugSet.size} campaign sources`;

  const gates = [
    {
      label: "External fanout clamp",
      value: `${adaptive.avgChapterOwnedSkipped.toFixed(1)} skipped/run`,
      target: "> 0 skipped external chapter sites on adaptive runs",
      passed: adaptive.runCount > 0 && adaptive.avgChapterOwnedSkipped > 0,
    },
    {
      label: "Broader-web clamp",
      value: `${adaptive.avgBroaderWebFollowed.toFixed(1)} followed/run`,
      target: "= 0 broader-web targets during chapter search",
      passed: adaptive.runCount > 0 && adaptive.avgBroaderWebFollowed === 0,
    },
    {
      label: "Institutional before wider web",
      value: `${adaptive.avgInstitutionalFollowed.toFixed(1)} institutional/run`,
      target: "institutional follow available before wider-web fanout",
      passed: adaptive.runCount > 0 && (adaptive.avgInstitutionalFollowed > 0 || adaptive.avgBroaderWebFollowed === 0),
    },
    {
      label: "Search wall time",
      value: `${Math.round(adaptive.avgWallTimeMs)}ms/run`,
      target: "<= 5000ms average chapter-search wall time",
      passed: adaptive.runCount > 0 && adaptive.avgWallTimeMs <= 5000,
    },
  ];

  return {
    scopeLabel,
    totalRuns: chapterSearchRuns.length,
    legacy,
    adaptive,
    deltas: {
      avgCanonicalCreated: adaptive.avgCanonicalCreated - legacy.avgCanonicalCreated,
      avgProvisionalCreated: adaptive.avgProvisionalCreated - legacy.avgProvisionalCreated,
      avgRejected: adaptive.avgRejected - legacy.avgRejected,
      avgNationalFollowed: adaptive.avgNationalFollowed - legacy.avgNationalFollowed,
      avgInstitutionalFollowed: adaptive.avgInstitutionalFollowed - legacy.avgInstitutionalFollowed,
      avgChapterOwnedSkipped: adaptive.avgChapterOwnedSkipped - legacy.avgChapterOwnedSkipped,
      avgBroaderWebFollowed: adaptive.avgBroaderWebFollowed - legacy.avgBroaderWebFollowed,
      avgWallTimeMs: adaptive.avgWallTimeMs - legacy.avgWallTimeMs,
    },
    gates,
  };
}

