import type {
  CampaignCohortSummary,
  CampaignProviderHealthHistoryPoint,
  CampaignReport,
  CampaignRun,
  CampaignRunItem
} from "@/lib/types";

function sum(items: number[]): number {
  return items.reduce((total, value) => total + value, 0);
}

function buildCohortSummary(cohort: "new" | "control", items: CampaignRunItem[]): CampaignCohortSummary {
  const relevant = items.filter((item) => item.cohort === cohort);
  const itemCount = relevant.length;
  const completedCount = relevant.filter((item) => item.status === "completed").length;
  const failedCount = relevant.filter((item) => item.status === "failed").length;
  const chaptersDiscovered = Math.max(sum(relevant.map((item) => item.scorecard.chaptersDiscovered)), 1);
  const anyContact = sum(relevant.map((item) => item.scorecard.chaptersWithAnyContact));
  const allThree = sum(relevant.map((item) => item.scorecard.chaptersWithAllThree));
  const websites = sum(relevant.map((item) => item.scorecard.websitesFound));
  const emails = sum(relevant.map((item) => item.scorecard.emailsFound));
  const instagrams = sum(relevant.map((item) => item.scorecard.instagramsFound));
  const jobs = sum(relevant.map((item) => item.scorecard.processedJobs + item.scorecard.requeuedJobs + item.scorecard.failedTerminalJobs));

  return {
    cohort,
    itemCount,
    completedCount,
    failedCount,
    anyContactSuccessRate: anyContact / chaptersDiscovered,
    allThreeSuccessRate: allThree / chaptersDiscovered,
    websiteCoverageRate: websites / chaptersDiscovered,
    emailCoverageRate: emails / chaptersDiscovered,
    instagramCoverageRate: instagrams / chaptersDiscovered,
    avgJobsPerItem: itemCount > 0 ? jobs / itemCount : 0
  };
}

function topFailureReasons(items: CampaignRunItem[]): Array<{ reason: string; count: number }> {
  const counts = new Map<string, number>();
  for (const item of items) {
    for (const entry of item.scorecard.failureHistogram) {
      counts.set(entry.reason, (counts.get(entry.reason) ?? 0) + entry.count);
    }
  }
  return [...counts.entries()]
    .map(([reason, count]) => ({ reason, count }))
    .sort((left, right) => right.count - left.count)
    .slice(0, 10);
}

function topSuccessfulHabits(items: CampaignRunItem[]): Array<{ label: string; value: number }> {
  const itemCount = Math.max(items.length, 1);
  return [
    { label: "avg source-native yield", value: sum(items.map((item) => item.scorecard.sourceNativeYield)) / itemCount },
    { label: "avg search efficiency", value: sum(items.map((item) => item.scorecard.searchEfficiency)) / itemCount },
    { label: "avg confidence quality", value: sum(items.map((item) => item.scorecard.confidenceQuality)) / itemCount },
    { label: "avg queue efficiency", value: sum(items.map((item) => item.scorecard.queueEfficiency)) / itemCount }
  ];
}

function recommendations(run: CampaignRun, history: CampaignProviderHealthHistoryPoint[], failures: Array<{ reason: string; count: number }>): string[] {
  const notes: string[] = [];
  const degradedCount = history.filter((point) => !point.healthy).length;
  const topFailure = failures[0]?.reason ?? null;

  if (degradedCount > 0) {
    notes.push("Provider health degraded during the campaign; consider lower active concurrency or stronger preflight requirements for the next run.");
  }
  if (run.summary.totalRequeued > run.summary.totalProcessed) {
    notes.push("Requeues exceeded processed work; reduce search-heavy concurrency or widen cooldowns before the next long-run campaign.");
  }
  if ((run.summary.anyContactSuccessRate ?? 0) < 0.5) {
    notes.push("Any-contact coverage is still below target; add more verified-source hints or improve low-yield source parsing before the next benchmark.");
  }
  if (topFailure?.includes("No chapters discovered")) {
    notes.push("Several sources produced zero discovered chapters; prioritize source-specific extraction hints or parser overrides for those nationals sites.");
  }
  if (notes.length === 0) {
    notes.push("Campaign metrics were stable overall; compare this run against a future control-heavy campaign to quantify recall improvements.");
  }
  return notes;
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

function buildLiveSummary(run: CampaignRun) {
  const itemCount = run.items.length;
  const completedCount = run.items.filter((item) => item.status === "completed").length;
  const failedCount = run.items.filter((item) => item.status === "failed").length;
  const skippedCount = run.items.filter((item) => item.status === "skipped").length;
  const activeCount = run.items.filter((item) => item.status === "running" || item.status === "queued" || item.status === "request_created").length;
  const totalChapters = Math.max(sum(run.items.map((item) => Math.max(item.scorecard.chaptersDiscovered, item.scorecard.chaptersWithAnyContact, item.scorecard.chaptersWithAllThree))), 1);
  const websitesFound = sum(run.items.map((item) => item.scorecard.websitesFound));
  const emailsFound = sum(run.items.map((item) => item.scorecard.emailsFound));
  const instagramsFound = sum(run.items.map((item) => item.scorecard.instagramsFound));
  const anyContact = sum(run.items.map((item) => item.scorecard.chaptersWithAnyContact));
  const allThree = sum(run.items.map((item) => item.scorecard.chaptersWithAllThree));
  const totalProcessed = sum(run.items.map((item) => item.scorecard.processedJobs));
  const totalRequeued = sum(run.items.map((item) => item.scorecard.requeuedJobs));
  const totalFailedTerminal = sum(run.items.map((item) => item.scorecard.failedTerminalJobs));
  const startedAtMs = resolveCampaignStartMs(run);
  const durationMs = startedAtMs
    ? Math.max((run.finishedAt ? new Date(run.finishedAt).getTime() : Date.now()) - startedAtMs, 1)
    : run.summary.durationMs;

  return {
    ...run.summary,
    targetCount: run.config.targetCount,
    itemCount,
    completedCount,
    failedCount,
    skippedCount,
    activeCount,
    anyContactSuccessRate: anyContact / totalChapters,
    allThreeSuccessRate: allThree / totalChapters,
    websiteCoverageRate: websitesFound / totalChapters,
    emailCoverageRate: emailsFound / totalChapters,
    instagramCoverageRate: instagramsFound / totalChapters,
    totalProcessed,
    totalRequeued,
    totalFailedTerminal,
    durationMs,
    jobsPerMinute: durationMs > 0 ? (totalProcessed * 60_000) / durationMs : 0
  };
}

export function buildCampaignReport(run: CampaignRun): CampaignReport {
  const providerHealthHistory = run.telemetry.providerHealthHistory ?? [];
  const cohortComparison = [buildCohortSummary("new", run.items), buildCohortSummary("control", run.items)];
  const failures = topFailureReasons(run.items);
  const summary = buildLiveSummary(run);

  return {
    campaignId: run.id,
    campaignName: run.name,
    generatedAt: new Date().toISOString(),
    summary,
    providerHealthHistory,
    cohortComparison,
    topFailureReasons: failures,
    topSuccessfulHabits: topSuccessfulHabits(run.items),
    recommendations: recommendations(run, providerHealthHistory, failures)
  };
}
