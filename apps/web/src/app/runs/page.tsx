import { StatusPill } from "@/components/status-pill";
import { TagPill } from "@/components/tag-pill";
import { fetchFromApi } from "@/lib/api-client";
import type { CrawlRunListItem } from "@/lib/types";

export default async function RunsPage() {
  const data = await fetchFromApi<CrawlRunListItem[]>("/api/runs?limit=200");

  return (
    <section className="panel">
      <h2>Crawl Runs</h2>
      <div className="tableWrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Source</th>
              <th>Status</th>
              <th>Strategy</th>
              <th>Page Confidence</th>
              <th>LLM Calls</th>
              <th>Seen</th>
              <th>Upserted</th>
              <th>Review</th>
              <th>Field Jobs</th>
              <th>Started</th>
            </tr>
          </thead>
          <tbody>
            {data.map((run) => (
              <tr key={run.id}>
                <td>{run.id}</td>
                <td>{run.sourceSlug ?? <span className="muted">n/a</span>}</td>
                <td>
                  <StatusPill status={run.status} />
                </td>
                <td>
                  {run.strategyUsed ? <TagPill label={run.strategyUsed} tone="info" /> : <span className="muted">n/a</span>}
                </td>
                <td>{run.pageLevelConfidence !== null ? run.pageLevelConfidence.toFixed(2) : <span className="muted">n/a</span>}</td>
                <td>{run.llmCallsUsed}</td>
                <td>{run.recordsSeen}</td>
                <td>{run.recordsUpserted}</td>
                <td>{run.reviewItemsCreated}</td>
                <td>{run.fieldJobsCreated}</td>
                <td>{new Date(run.startedAt).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
