import { PageIntro } from "@/components/page-intro";
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
    <div className="sectionStack">
      <PageIntro
        eyebrow="Review"
        title="Manual decision queue for low-confidence data"
        description="This page is for triaging ambiguous records and checking the field-job backlog before questionable data reaches the chapter table."
        meta={[`${reviewItems.length} review items`, `${fieldJobs.length} field jobs`, `${reviewItems.filter((item) => item.status === "open").length} open`]}
      />

      <section className="panel">
        <h2>Review Queue</h2>
        <p className="sectionDescription">Resolve ambiguous extractions here, then use the field jobs table below to inspect what enrichment work is still in flight.</p>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Source</th>
                <th>Type</th>
                <th>Status</th>
                <th>Reason</th>
                <th>Candidate</th>
                <th>Confidence</th>
                <th>Source Link</th>
                <th>Query</th>
                <th>Rejection Summary</th>
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
                  <td className="monoCell">
                    {item.candidateValue ? item.candidateValue : <span className="muted">none</span>}
                  </td>
                  <td>{item.confidence !== null ? item.confidence.toFixed(2) : <span className="muted">n/a</span>}</td>
                  <td>
                    {item.sourceUrl ? (
                      <a href={item.sourceUrl} target="_blank" rel="noreferrer">
                        Open Source
                      </a>
                    ) : (
                      <span className="muted">n/a</span>
                    )}
                  </td>
                  <td className="monoCell">{item.query ?? <span className="muted">n/a</span>}</td>
                  <td>
                    {item.rejectionSummary ? (
                      <>
                        <strong>{item.rejectionSummary.totalRejections}</strong> rejected
                        <br />
                        <span className="muted">{item.rejectionSummary.uniqueReasons} reasons</span>
                        <br />
                        <span className="muted">
                          {item.rejectionSummary.topReasons
                            .slice(0, 2)
                            .map((entry) => `${entry.reason} (${entry.count})`)
                            .join(", ")}
                        </span>
                      </>
                    ) : (
                      <span className="muted">n/a</span>
                    )}
                  </td>
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
      </section>

      <section className="panel">
        <h2>Field Jobs</h2>
        <p className="sectionDescription">Monitor the queued enrichment work here so you can tell whether the pipeline is discovering websites, emails, and social links as expected.</p>
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
    </div>
  );
}
