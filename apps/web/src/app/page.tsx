import { MetricCard } from "@/components/metric-card";
import { PageIntro } from "@/components/page-intro";
import { StatusPill } from "@/components/status-pill";
import { TagPill } from "@/components/tag-pill";
import { APP_VERSION_LABEL } from "@/lib/platform-version";
import type { AgentOpsSummary, CampaignRun, ChapterListItem, CrawlRunListItem, FieldJobListItem, ReviewItemListItem } from "@/lib/types";
import { getAgentOpsSummary } from "@/lib/repositories/agent-ops-repository";
import { listCampaignRuns } from "@/lib/repositories/campaign-run-repository";
import { listChapters } from "@/lib/repositories/chapter-repository";
import { listCrawlRuns } from "@/lib/repositories/crawl-run-repository";
import { listFieldJobs } from "@/lib/repositories/field-job-repository";
import { listReviewItems } from "@/lib/repositories/review-item-repository";

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
  const [chapters, runs, reviewItems, fieldJobs, campaigns, agentOps] = await Promise.all([
    listChapters({ limit: 5, offset: 0 }) as Promise<ChapterListItem[]>,
    listCrawlRuns(8) as Promise<CrawlRunListItem[]>,
    listReviewItems(8) as Promise<ReviewItemListItem[]>,
    listFieldJobs(8) as Promise<FieldJobListItem[]>,
    listCampaignRuns(8) as Promise<CampaignRun[]>,
    getAgentOpsSummary() as Promise<AgentOpsSummary>
  ]);

  const latestRun = runs[0];

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Overview"
        title={`${APP_VERSION_LABEL} command center for sourcing, agents, and review`}
        description={`Use this page to get your bearings fast: whether the ${APP_VERSION_LABEL} worker queue is healthy, how much data is loaded, and where operator attention is needed next.`}
        meta={[
          `${chapters.length} preview chapters`,
          `${runs.length} recent runs`,
          `${campaigns.filter((item) => item.status === "running" || item.status === "queued").length} active campaigns`,
          agentOps.requestQueueQueued === 0 && agentOps.requestQueueRunning === 0 ? `${APP_VERSION_LABEL} queue clear` : `${APP_VERSION_LABEL} queue active`
        ]}
      />

      <section className="panel">
        <h2>System Snapshot</h2>
        <div className="metrics">
          <MetricCard label="Chapters" value={chapters.length} />
          <MetricCard label="Recent Runs" value={runs.length} />
          <MetricCard label="Campaigns" value={campaigns.length} />
          <MetricCard label="Open Reviews" value={reviewItems.filter((item) => item.status === "open").length} />
          <MetricCard label="Queued Jobs" value={fieldJobs.filter((item) => item.status === "queued").length} />
          <MetricCard label="V3 Graph Runs" value={agentOps.graphRunsTotal} />
          <MetricCard label="Queued Requests" value={agentOps.requestQueueQueued} />
          <MetricCard label="Provisional Chapters" value={agentOps.provisionalOpen} />
        </div>
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



