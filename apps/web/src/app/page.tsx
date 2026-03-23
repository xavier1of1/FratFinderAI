import { MetricCard } from "@/components/metric-card";
import { StatusPill } from "@/components/status-pill";
import { TagPill } from "@/components/tag-pill";
import { fetchFromApi } from "@/lib/api-client";
import type { ChapterListItem, CrawlRunListItem, FieldJobListItem, ReviewItemListItem } from "@/lib/types";

export default async function OverviewPage() {
  const [chapters, runs, reviewItems, fieldJobs] = await Promise.all([
    fetchFromApi<ChapterListItem[]>("/api/chapters?limit=5"),
    fetchFromApi<CrawlRunListItem[]>("/api/runs?limit=8"),
    fetchFromApi<ReviewItemListItem[]>("/api/review-items?limit=8"),
    fetchFromApi<FieldJobListItem[]>("/api/field-jobs?limit=8")
  ]);

  const latestRun = runs[0];

  return (
    <section className="panel">
      <h2>System Snapshot</h2>
      <div className="metrics">
        <MetricCard label="Chapters" value={chapters.length} />
        <MetricCard label="Recent Runs" value={runs.length} />
        <MetricCard label="Open Reviews" value={reviewItems.filter((item) => item.status === "open").length} />
        <MetricCard label="Queued Jobs" value={fieldJobs.filter((item) => item.status === "queued").length} />
      </div>

      <h2>Latest Crawl Run</h2>
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
  );
}
