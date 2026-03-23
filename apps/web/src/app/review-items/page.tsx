import { StatusPill } from "@/components/status-pill";
import { ReviewStatusForm } from "@/components/review-status-form";
import { fetchFromApi } from "@/lib/api-client";
import type { FieldJobListItem, ReviewItemListItem } from "@/lib/types";

export default async function ReviewItemsPage() {
  const [reviewItems, fieldJobs] = await Promise.all([
    fetchFromApi<ReviewItemListItem[]>("/api/review-items?limit=200"),
    fetchFromApi<FieldJobListItem[]>("/api/field-jobs?limit=200")
  ]);

  return (
    <section className="panel">
      <h2>Review Queue</h2>
      <div className="tableWrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Source</th>
              <th>Type</th>
              <th>Status</th>
              <th>Reason</th>
              <th>Extraction Notes</th>
              <th>Last Action</th>
              <th>Action</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {reviewItems.map((item) => (
              <tr key={item.id}>
                <td>{item.id.slice(0, 8)}</td>
                <td>{item.sourceSlug ?? <span className="muted">n/a</span>}</td>
                <td>{item.itemType}</td>
                <td>
                  <StatusPill status={item.status} />
                </td>
                <td>{item.reason}</td>
                <td>{item.extractionNotes ?? <span className="muted">none</span>}</td>
                <td>
                  {item.lastAction ? (
                    <>
                      {item.lastAction} by {item.lastActor ?? "unknown"}
                      <br />
                      <span className="muted">{item.lastActionAt ? new Date(item.lastActionAt).toLocaleString() : ""}</span>
                    </>
                  ) : (
                    <span className="muted">No audit yet</span>
                  )}
                </td>
                <td>
                  <ReviewStatusForm id={item.id} currentStatus={item.status as "open" | "triaged" | "resolved" | "ignored"} />
                </td>
                <td>{new Date(item.createdAt).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2 style={{ marginTop: "1.5rem" }}>Field Jobs</h2>
      <div className="tableWrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Chapter</th>
              <th>Field</th>
              <th>Status</th>
              <th>Attempts</th>
              <th>Worker</th>
              <th>Scheduled</th>
              <th>Last Error</th>
            </tr>
          </thead>
          <tbody>
            {fieldJobs.map((job) => (
              <tr key={job.id}>
                <td>{job.id.slice(0, 8)}</td>
                <td>{job.chapterSlug}</td>
                <td>{job.fieldName}</td>
                <td>
                  <StatusPill status={job.status} />
                </td>
                <td>
                  {job.attempts}/{job.maxAttempts}
                </td>
                <td>{job.claimedBy ?? <span className="muted">unclaimed</span>}</td>
                <td>{new Date(job.scheduledAt).toLocaleString()}</td>
                <td>{job.lastError ?? <span className="muted">none</span>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
