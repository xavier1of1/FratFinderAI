import { MetricCard } from "@/components/metric-card";
import { PageIntro } from "@/components/page-intro";
import { StatusPill } from "@/components/status-pill";
import { TagPill } from "@/components/tag-pill";
import { APP_VERSION_LABEL } from "@/lib/platform-version";
import type { AgentOpsSummary, CrawlRunListItem } from "@/lib/types";
import { getAgentOpsSummary } from "@/lib/repositories/agent-ops-repository";
import { getCampaignRunCounts } from "@/lib/repositories/campaign-run-repository";
import { getChapterListMetadata } from "@/lib/repositories/chapter-repository";
import { getCrawlRunCounts, listCrawlRuns } from "@/lib/repositories/crawl-run-repository";
import { getReviewItemStatusCounts } from "@/lib/repositories/review-item-repository";

const pageGuide = [
  {
    href: "/chapters",
    title: "Chapters",
    description: "Browse loaded chapters, scan contact coverage, and filter the map and table for the schools you want to show."
  },
  {
    href: "/nationals",
    title: "Nationals",
    description: "Inspect national-organization profiles separately from chapter rows so HQ contact never silently masquerades as chapter contact."
  },
  {
    href: "/runs",
    title: "Crawl Runs",
    description: "Audit how each source performed, which strategy fired, and how many records and follow-up jobs were created."
  },
  {
    href: "/agent-ops",
    title: "Agent Ops",
    description: "Inspect V3 request graph runs, provisional chapter discoveries, and the evidence ledger behind adaptive writes."
  },
  {
    href: "/review-items",
    title: "Review Queue",
    description: "Triage ambiguous extractions, inspect field jobs, and keep the pipeline trustworthy before data is promoted."
  },
  {
    href: "/benchmarks",
    title: "Benchmarks",
    description: "Launch and compare benchmark runs, track throughput, and spot regressions with cycle-level metrics."
  },
  {
    href: "/campaigns",
    title: "Campaigns",
    description: "Run long multi-fraternity validations, monitor provider health, and compare coverage and throughput over time."
  },
  {
    href: "/fraternity-intake",
    title: "Fraternity Intake",
    description: "Submit fraternity crawl requests, confirm discovered sources, and monitor staged progress through enrichment."
  }
];

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const [chapterMetadata, runs, crawlRunCounts, reviewCounts, campaignCounts, agentOps] = await Promise.all([
    getChapterListMetadata({}),
    listCrawlRuns(1) as Promise<CrawlRunListItem[]>,
    getCrawlRunCounts(),
    getReviewItemStatusCounts(),
    getCampaignRunCounts(),
    getAgentOpsSummary() as Promise<AgentOpsSummary>
  ]);

  const latestRun = runs[0];
  const activeCampaigns = campaignCounts.queued + campaignCounts.running;
  const queueHealthy = agentOps.fieldJobsActionable === 0 || agentOps.fieldJobWorkerAlertOpen === 0;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Overview"
        title={`${APP_VERSION_LABEL} command center for sourcing, agents, and review`}
        description={`Use this page to get your bearings fast: whether the ${APP_VERSION_LABEL} worker queue is healthy, how much data is loaded, and where operator attention is needed next.`}
        meta={[
          `${chapterMetadata.totalCount} total chapters`,
          `${crawlRunCounts.total} total crawl runs`,
          `${activeCampaigns} active campaigns`,
          queueHealthy ? `${APP_VERSION_LABEL} queue healthy` : `${APP_VERSION_LABEL} queue needs attention`
        ]}
      />

      <section className="panel">
        <h2>System Snapshot</h2>
        <p className="sectionDescription">These cards are sourced from aggregate backend queries, not preview-list lengths, so they reflect the real system state.</p>
        <div className="metrics">
          <MetricCard label="Total Chapters" value={agentOps.accuracyRecovery.totalChapters} />
          <MetricCard label="Complete Rows" value={agentOps.accuracyRecovery.completeRows} />
          <MetricCard label="Active Rows With Any Contact" value={agentOps.accuracyRecovery.activeRowsWithAnyContact} />
          <MetricCard label="Open Reviews" value={reviewCounts.open} />
          <MetricCard label="Total Crawl Runs" value={crawlRunCounts.total} />
          <MetricCard label="Active Campaigns" value={activeCampaigns} />
          <MetricCard label="Queued Field Jobs" value={agentOps.fieldJobsQueued} />
          <MetricCard label="Actionable Field Jobs" value={agentOps.fieldJobsActionable} />
          <MetricCard label="Queued Requests" value={agentOps.requestQueueQueued} />
          <MetricCard label="Provisional Chapters" value={agentOps.provisionalOpen} />
        </div>
      </section>

      <section className="panel">
        <h2>Accuracy And Queue Health</h2>
        <div className="metrics">
          <MetricCard label="Chapter-Specific Contact Rows" value={agentOps.accuracyRecovery.chapterSpecificContactRows} />
          <MetricCard label="Nationals-Only Contact Rows" value={agentOps.accuracyRecovery.nationalsOnlyContactRows} />
          <MetricCard label="Validated Inactive Rows" value={agentOps.accuracyRecovery.inactiveValidatedRows} />
          <MetricCard label="Confirmed-Absent Websites" value={agentOps.accuracyRecovery.confirmedAbsentWebsiteRows} />
          <MetricCard label="Blocked Provider Jobs" value={agentOps.fieldJobsBlockedProvider} />
          <MetricCard label="Blocked Dependency Jobs" value={agentOps.fieldJobsBlockedDependency} />
          <MetricCard label="Repair Backlog" value={agentOps.fieldJobsRepairBacklog} />
          <MetricCard label="Field Workers Active" value={agentOps.fieldJobWorkersActive} />
        </div>
        <p>
          <StatusPill status={queueHealthy ? "healthy" : "warning"} /> <span className="muted"> Queue:</span>{" "}
          {queueHealthy
            ? "actionable work is covered by active workers or there is no hot backlog."
            : "actionable work exists without enough active workers, so the operator alert path is open."}
        </p>
      </section>

      <section className="panel">
        <h2>Latest Crawl Run</h2>
        <p className="sectionDescription">This is the fastest way to see whether the newest source run produced useful records or needs intervention.</p>
        {latestRun ? (
          <p>
            <StatusPill status={latestRun.status} /> <span className="muted"> Source:</span> {latestRun.sourceSlug ?? "n/a"}
            {latestRun.runtimeMode ? (<><span className="muted"> | Runtime:</span> <TagPill label={latestRun.runtimeMode} tone="info" /></>) : null}
            {latestRun.strategyUsed ? (
              <>
                <span className="muted"> | Strategy:</span> <TagPill label={latestRun.strategyUsed} tone="info" />
              </>
            ) : null}
            {latestRun.pageLevelConfidence !== null ? (
              <>
                <span className="muted"> | Confidence:</span> {latestRun.pageLevelConfidence.toFixed(2)}
              </>
            ) : null}
            <span className="muted"> | Upserted:</span> {latestRun.recordsUpserted}
            <span className="muted"> | Review Items:</span> {latestRun.reviewItemsCreated}
            {latestRun.stopReason ? (<><span className="muted"> | Stop:</span> {latestRun.stopReason}</>) : null}
          </p>
        ) : (
          <p className="muted">No crawl runs found yet.</p>
        )}
      </section>

      <section className="panel">
        <h2>What Each Page Is For</h2>
        <div className="guideGrid">
          {pageGuide.map((item) => (
            <a key={item.href} href={item.href} className="guideCard">
              <p className="guideEyebrow">Workspace</p>
              <h3>{item.title}</h3>
              <p>{item.description}</p>
            </a>
          ))}
        </div>
      </section>
    </div>
  );
}



