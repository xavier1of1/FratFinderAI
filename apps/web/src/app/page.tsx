import Link from "next/link";

import { MetricCard } from "@/components/metric-card";
import { PageIntro } from "@/components/page-intro";
import { StatusPill } from "@/components/status-pill";
import { TagPill } from "@/components/tag-pill";
import { fetchFromApi } from "@/lib/api-client";
import type { ChapterListItem, CrawlRunListItem, FieldJobListItem, ReviewItemListItem } from "@/lib/types";

const pageGuide = [
  {
    href: "/chapters",
    title: "Chapters",
    description: "Browse loaded chapters, scan contact coverage, and filter the map and table for the schools you want to show."
  },
  {
    href: "/runs",
    title: "Crawl Runs",
    description: "Audit how each source performed, which strategy fired, and how many records and follow-up jobs were created."
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
    href: "/fraternity-intake",
    title: "Fraternity Intake",
    description: "Submit fraternity crawl requests, confirm discovered sources, and monitor staged progress through enrichment."
  }
];

export default async function OverviewPage() {
  const [chapters, runs, reviewItems, fieldJobs] = await Promise.all([
    fetchFromApi<ChapterListItem[]>("/api/chapters?limit=5"),
    fetchFromApi<CrawlRunListItem[]>("/api/runs?limit=8"),
    fetchFromApi<ReviewItemListItem[]>("/api/review-items?limit=8"),
    fetchFromApi<FieldJobListItem[]>("/api/field-jobs?limit=8")
  ]);

  const latestRun = runs[0];

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Overview"
        title="Command center for sourcing, enrichment, and review"
        description="Use this page to get your bearings fast: how much data is loaded, whether crawls are healthy, and where manual attention is needed next."
        meta={[`${chapters.length} preview chapters`, `${runs.length} recent runs`, `${fieldJobs.filter((item) => item.status === "queued").length} queued jobs`]}
      />

      <section className="panel">
        <h2>System Snapshot</h2>
        <div className="metrics">
          <MetricCard label="Chapters" value={chapters.length} />
          <MetricCard label="Recent Runs" value={runs.length} />
          <MetricCard label="Open Reviews" value={reviewItems.filter((item) => item.status === "open").length} />
          <MetricCard label="Queued Jobs" value={fieldJobs.filter((item) => item.status === "queued").length} />
        </div>
      </section>

      <section className="panel">
        <h2>Latest Crawl Run</h2>
        <p className="sectionDescription">This is the fastest way to see whether the newest source run produced useful records or needs intervention.</p>
        {latestRun ? (
          <p>
            <StatusPill status={latestRun.status} /> <span className="muted"> Source:</span> {latestRun.sourceSlug ?? "n/a"}
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
          </p>
        ) : (
          <p className="muted">No crawl runs found yet.</p>
        )}
      </section>

      <section className="panel">
        <h2>What Each Page Is For</h2>
        <div className="guideGrid">
          {pageGuide.map((item) => (
            <Link key={item.href} href={item.href} className="guideCard">
              <p className="guideEyebrow">Workspace</p>
              <h3>{item.title}</h3>
              <p>{item.description}</p>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}