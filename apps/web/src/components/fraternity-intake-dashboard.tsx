"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

import { StatusPill } from "@/components/status-pill";
import type { FraternityCrawlRequest, FraternityCrawlRequestStatus } from "@/lib/types";

interface ApiSuccess<T> {
  success: true;
  data: T;
}

interface ApiFailure {
  success: false;
  error: {
    code: string;
    message: string;
    requestId: string;
  };
}

type ApiEnvelope<T> = ApiSuccess<T> | ApiFailure;

function sortRequests(items: FraternityCrawlRequest[]): FraternityCrawlRequest[] {
  return [...items].sort((left, right) => new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime());
}

function toDateTimeLocalValue(iso: string): string {
  const date = new Date(iso);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function fromDateTimeLocalValue(value: string): string {
  return new Date(value).toISOString();
}

function formatConfidence(value: unknown): string {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "n/a";
  }
  return numeric.toFixed(2);
}

async function fetchRequests(): Promise<FraternityCrawlRequest[]> {
  const response = await fetch("/api/fraternity-crawl-requests?limit=200", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<FraternityCrawlRequest[]>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch requests: ${response.status}`);
  }

  return sortRequests(payload.data);
}

export function FraternityIntakeDashboard({ initialRequests }: { initialRequests: FraternityCrawlRequest[] }) {
  const [requests, setRequests] = useState<FraternityCrawlRequest[]>(sortRequests(initialRequests));
  const [selectedId, setSelectedId] = useState<string | null>(initialRequests[0]?.id ?? null);
  const [statusFilter, setStatusFilter] = useState<FraternityCrawlRequestStatus | "all">("all");
  const [fraternityName, setFraternityName] = useState("");
  const [scheduledFor, setScheduledFor] = useState("");
  const [sourceOverride, setSourceOverride] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const filteredRequests = useMemo(() => {
    const sorted = sortRequests(requests);
    if (statusFilter === "all") {
      return sorted;
    }
    return sorted.filter((item) => item.status === statusFilter);
  }, [requests, statusFilter]);

  const selectedRequest = useMemo(() => {
    if (!filteredRequests.length) {
      return null;
    }
    if (!selectedId) {
      return filteredRequests[0] ?? null;
    }
    return filteredRequests.find((item) => item.id === selectedId) ?? filteredRequests[0] ?? null;
  }, [filteredRequests, selectedId]);

  useEffect(() => {
    if (!selectedRequest) {
      setSourceOverride("");
      return;
    }
    const suggested =
      selectedRequest.sourceUrl ??
      selectedRequest.progress.discovery?.sourceUrl ??
      selectedRequest.progress.discovery?.candidates?.[0]?.url ??
      "";
    setSourceOverride(suggested);
  }, [selectedRequest?.id]);

  async function refresh(selectNewest = false) {
    setIsRefreshing(true);
    try {
      const data = await fetchRequests();
      setRequests(data);
      if (selectNewest && data[0]) {
        setSelectedId(data[0].id);
      } else if (selectedId && !data.some((item) => item.id === selectedId)) {
        setSelectedId(data[0]?.id ?? null);
      }
      setErrorMessage(null);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsRefreshing(false);
    }
  }

  async function submitRequest(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSubmitting(true);
    setErrorMessage(null);

    try {
      const payload: Record<string, unknown> = {
        fraternityName: fraternityName.trim()
      };
      if (scheduledFor.trim()) {
        payload.scheduledFor = fromDateTimeLocalValue(scheduledFor.trim());
      }

      const response = await fetch("/api/fraternity-crawl-requests", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const result = (await response.json()) as ApiEnvelope<FraternityCrawlRequest>;
      if (!response.ok || !result.success) {
        if (!result.success) {
          throw new Error(`${result.error.code}: ${result.error.message}`);
        }
        throw new Error(`Failed to create request (${response.status})`);
      }

      setFraternityName("");
      setScheduledFor("");
      await refresh(true);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSubmitting(false);
    }
  }

  async function runAction(
    action: "confirm" | "cancel" | "reschedule",
    request: FraternityCrawlRequest,
    options?: { sourceUrl?: string }
  ) {
    try {
      setErrorMessage(null);

      const payload: Record<string, unknown> = { action };
      if (action === "reschedule") {
        payload.scheduledFor = new Date(Date.now() + 5 * 60_000).toISOString();
      }
      if (action === "confirm" && options?.sourceUrl?.trim()) {
        payload.sourceUrl = options.sourceUrl.trim();
      }

      const response = await fetch(`/api/fraternity-crawl-requests/${request.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const result = (await response.json()) as ApiEnvelope<FraternityCrawlRequest>;
      if (!response.ok || !result.success) {
        if (!result.success) {
          throw new Error(`${result.error.code}: ${result.error.message}`);
        }
        throw new Error(`Failed to ${action} request (${response.status})`);
      }

      await refresh();
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    }
  }

  async function expedite(request: FraternityCrawlRequest) {
    try {
      setErrorMessage(null);

      const response = await fetch(`/api/fraternity-crawl-requests/${request.id}/expedite`, {
        method: "POST"
      });

      const result = (await response.json()) as ApiEnvelope<FraternityCrawlRequest>;
      if (!response.ok || !result.success) {
        if (!result.success) {
          throw new Error(`${result.error.code}: ${result.error.message}`);
        }
        throw new Error(`Failed to expedite request (${response.status})`);
      }

      await refresh();
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    }
  }

  const activeCount = requests.filter((item) => item.status === "queued" || item.status === "running").length;

  return (
    <div className="sectionStack">
      <section className="panel">
        <h2>Suggest Fraternity Crawl</h2>
        <p className="sectionDescription">
          Enter a fraternity name to discover its national source, queue staged crawling, and track website/email/Instagram enrichment progress.
        </p>

        <form onSubmit={submitRequest}>
          <div className="benchmarkFormGrid">
            <div className="fieldStack">
              <label htmlFor="fraternity-name">Fraternity Name</label>
              <input
                id="fraternity-name"
                placeholder="Lambda Chi Alpha"
                value={fraternityName}
                onChange={(event) => setFraternityName(event.target.value)}
                required
              />
            </div>

            <div className="fieldStack">
              <label htmlFor="scheduled-for">Scheduled Start</label>
              <input
                id="scheduled-for"
                type="datetime-local"
                value={scheduledFor}
                onChange={(event) => setScheduledFor(event.target.value)}
              />
            </div>
          </div>

          <div className="buttonRow">
            <button type="submit" className="buttonPrimaryAuto" disabled={isSubmitting}>
              {isSubmitting ? "Submitting..." : "Create Request"}
            </button>
            <button type="button" className="buttonSecondary" onClick={() => void refresh()} disabled={isRefreshing}>
              {isRefreshing ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </form>

        {errorMessage ? <p className="benchmarkError">{errorMessage}</p> : null}
      </section>

      <section className="panel">
        <h2>Request Snapshot</h2>
        <div className="metrics">
          <div className="metricCard">
            <p className="metricLabel">Total Requests</p>
            <p className="metricValue">{requests.length}</p>
          </div>
          <div className="metricCard">
            <p className="metricLabel">Queued / Running</p>
            <p className="metricValue">{activeCount}</p>
          </div>
          <div className="metricCard">
            <p className="metricLabel">Latest</p>
            <p className="metricValue">{requests[0]?.fraternityName ?? "n/a"}</p>
          </div>
        </div>
      </section>

      <section className="benchmarkLayout">
        <article className="panel">
          <h2>All Requests</h2>

          <div className="buttonRow">
            <button type="button" className={`buttonSecondary ${statusFilter === "all" ? "isActiveFilter" : ""}`} onClick={() => setStatusFilter("all")}>All</button>
            <button type="button" className={`buttonSecondary ${statusFilter === "draft" ? "isActiveFilter" : ""}`} onClick={() => setStatusFilter("draft")}>Draft</button>
            <button type="button" className={`buttonSecondary ${statusFilter === "queued" ? "isActiveFilter" : ""}`} onClick={() => setStatusFilter("queued")}>Queued</button>
            <button type="button" className={`buttonSecondary ${statusFilter === "running" ? "isActiveFilter" : ""}`} onClick={() => setStatusFilter("running")}>Running</button>
            <button type="button" className={`buttonSecondary ${statusFilter === "succeeded" ? "isActiveFilter" : ""}`} onClick={() => setStatusFilter("succeeded")}>Succeeded</button>
            <button type="button" className={`buttonSecondary ${statusFilter === "failed" ? "isActiveFilter" : ""}`} onClick={() => setStatusFilter("failed")}>Failed</button>
          </div>

          {filteredRequests.length === 0 ? (
            <p className="muted">No requests yet.</p>
          ) : (
            <div className="benchmarkList">
              {filteredRequests.map((item) => (
                <button
                  type="button"
                  key={item.id}
                  className={`benchmarkListItem${item.id === selectedRequest?.id ? " active" : ""}`}
                  onClick={() => setSelectedId(item.id)}
                >
                  <div className="benchmarkListItemHeader">
                    <strong>{item.fraternityName}</strong>
                    <StatusPill status={item.status} />
                  </div>
                  <div className="benchmarkListMeta">
                    <span>{item.stage}</span>
                    <span>{new Date(item.scheduledFor).toLocaleString()}</span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </article>

        <article className="panel">
          <h2>Request Details</h2>
          {selectedRequest ? (
            <>
              <div className="benchmarkSelectedHeader">
                <div>
                  <h3>{selectedRequest.fraternityName}</h3>
                  <p className="muted">{selectedRequest.sourceSlug ?? "Source pending confirmation"}</p>
                </div>
                <StatusPill status={selectedRequest.status} />
              </div>

              <div className="benchmarkMetaGrid">
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Stage</p>
                  <p className="benchmarkMetaValue">{selectedRequest.stage}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Scheduled</p>
                  <p className="benchmarkMetaValue">{new Date(selectedRequest.scheduledFor).toLocaleString()}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Discovery Confidence</p>
                  <p className="benchmarkMetaValue">{formatConfidence(selectedRequest.sourceConfidence)}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Discovery Provenance</p>
                  <p className="benchmarkMetaValue">{selectedRequest.progress.discovery?.sourceProvenance ?? "n/a"}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Fallback Reason</p>
                  <p className="benchmarkMetaValue">{selectedRequest.progress.discovery?.fallbackReason ?? "n/a"}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Source Link</p>
                  <p className="benchmarkMetaValue">
                    {selectedRequest.sourceUrl ? (
                      <a href={selectedRequest.sourceUrl} target="_blank" rel="noreferrer">
                        Open Source
                      </a>
                    ) : (
                      "n/a"
                    )}
                  </p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Chapters Discovered</p>
                  <p className="benchmarkMetaValue">{selectedRequest.progress.crawlRun?.recordsSeen ?? 0}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Field Jobs Created</p>
                  <p className="benchmarkMetaValue">{selectedRequest.progress.crawlRun?.fieldJobsCreated ?? 0}</p>
                </div>
              </div>

              <h3>Discovery Review</h3>
              <p className="muted">
                Discovery runs deterministic source search and ranks candidates by domain trust + fraternity context. Confirm uses the URL below.
              </p>
              <div className="benchmarkFormGrid">
                <div className="fieldStack">
                  <label htmlFor="source-override">Source URL Override</label>
                  <input
                    id="source-override"
                    type="url"
                    value={sourceOverride}
                    placeholder="https://example.org/chapter-directory/"
                    onChange={(event) => setSourceOverride(event.target.value)}
                  />
                </div>
              </div>
              {sourceOverride ? (
                <p className="muted">
                  Candidate source:{" "}
                  <a href={sourceOverride} target="_blank" rel="noreferrer">
                    {sourceOverride}
                  </a>
                </p>
              ) : null}

              {selectedRequest.progress.discovery?.candidates?.length ? (
                <div className="tableWrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Score</th>
                        <th>Provider</th>
                        <th>URL</th>
                        <th>Title</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {selectedRequest.progress.discovery.candidates.map((candidate) => (
                        <tr key={`${candidate.url}-${candidate.rank}`}>
                          <td>{formatConfidence(candidate.score)}</td>
                          <td>{candidate.provider}</td>
                          <td>
                            <a href={candidate.url} target="_blank" rel="noreferrer">
                              {candidate.url}
                            </a>
                          </td>
                          <td>{candidate.title}</td>
                          <td>
                            <button type="button" className="buttonSecondary" onClick={() => setSourceOverride(candidate.url)}>
                              Use URL
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="muted">No discovery candidates were retained for this request.</p>
              )}

              <h3>Resolution Trace</h3>
              {selectedRequest.progress.discovery?.resolutionTrace?.length ? (
                <div className="tableWrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Step</th>
                        <th>Details</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedRequest.progress.discovery.resolutionTrace.map((traceStep, index) => (
                        <tr key={`${selectedRequest.id}-trace-${index}`}>
                          <td>{String(traceStep.step ?? `step-${index + 1}`)}</td>
                          <td>
                            <code>{JSON.stringify(traceStep)}</code>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="muted">No resolution trace captured yet.</p>
              )}

              <div className="buttonRow">
                {selectedRequest.status === "draft" ? (
                  <button
                    type="button"
                    className="buttonPrimaryAuto"
                    onClick={() => void runAction("confirm", selectedRequest, { sourceUrl: sourceOverride })}
                  >
                    Confirm
                  </button>
                ) : null}
                {selectedRequest.status === "queued" || selectedRequest.status === "running" ? (
                  <button type="button" className="buttonPrimaryAuto" onClick={() => void expedite(selectedRequest)}>
                    Expedite
                  </button>
                ) : null}
                {selectedRequest.status !== "canceled" && selectedRequest.status !== "succeeded" ? (
                  <button type="button" className="buttonSecondary" onClick={() => void runAction("cancel", selectedRequest)}>
                    Cancel
                  </button>
                ) : null}
                {selectedRequest.status !== "running" && selectedRequest.status !== "succeeded" ? (
                  <button type="button" className="buttonSecondary" onClick={() => void runAction("reschedule", selectedRequest)}>
                    Reschedule +5m
                  </button>
                ) : null}
              </div>

              <h3>Field Job Progress</h3>
              <div className="tableWrap">
                <table>
                  <thead>
                    <tr>
                      <th>Field</th>
                      <th>Queued</th>
                      <th>Running</th>
                      <th>Done</th>
                      <th>Failed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(["find_website", "find_email", "find_instagram"] as const).map((field) => {
                      const snapshot = selectedRequest.progress.fields?.[field] ?? { queued: 0, running: 0, done: 0, failed: 0 };
                      return (
                        <tr key={field}>
                          <td>{field}</td>
                          <td>{snapshot.queued ?? 0}</td>
                          <td>{snapshot.running ?? 0}</td>
                          <td>{snapshot.done ?? 0}</td>
                          <td>{snapshot.failed ?? 0}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              <h3>Stage Timeline</h3>
              {selectedRequest.events.length === 0 ? (
                <p className="muted">No events yet.</p>
              ) : (
                <div className="tableWrap">
                  <table>
                    <thead>
                      <tr>
                        <th>When</th>
                        <th>Event</th>
                        <th>Message</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedRequest.events.map((event) => (
                        <tr key={event.id}>
                          <td>{new Date(event.createdAt).toLocaleString()}</td>
                          <td>{event.eventType}</td>
                          <td>{event.message}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {selectedRequest.lastError ? <p className="benchmarkError">{selectedRequest.lastError}</p> : null}
            </>
          ) : (
            <p className="muted">Select a request to inspect details.</p>
          )}
        </article>
      </section>
    </div>
  );
}
