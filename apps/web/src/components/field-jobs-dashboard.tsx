"use client";

import { useEffect, useState } from "react";

import { StatusPill } from "@/components/status-pill";
import type { FieldJobListItem, FieldJobLogFeed } from "@/lib/types";

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

const JOB_REFRESH_MS = 7_000;
const LOG_REFRESH_MS = 4_000;

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "n/a";
  }
  return new Date(value).toLocaleString();
}

async function fetchFieldJobs(): Promise<FieldJobListItem[]> {
  const response = await fetch("/api/field-jobs?limit=200", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<FieldJobListItem[]>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch field jobs: ${response.status}`);
  }
  return payload.data;
}

async function fetchFieldJobLogs(jobId: string): Promise<FieldJobLogFeed> {
  const response = await fetch(`/api/field-jobs/${jobId}/logs?limit=80`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<FieldJobLogFeed>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch field job logs: ${response.status}`);
  }
  return payload.data;
}

export function FieldJobsDashboard({ initialJobs }: { initialJobs: FieldJobListItem[] }) {
  const [jobs, setJobs] = useState<FieldJobListItem[]>(initialJobs);
  const [liveStreaming, setLiveStreaming] = useState(false);
  const [openJobIds, setOpenJobIds] = useState<string[]>([]);
  const [feeds, setFeeds] = useState<Record<string, FieldJobLogFeed>>({});
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!liveStreaming) {
      return;
    }

    let cancelled = false;
    const refreshJobs = async () => {
      try {
        const nextJobs = await fetchFieldJobs();
        if (!cancelled) {
          setJobs(nextJobs);
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(error instanceof Error ? error.message : String(error));
        }
      }
    };

    refreshJobs();
    const timer = window.setInterval(refreshJobs, JOB_REFRESH_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [liveStreaming]);

  useEffect(() => {
    if (!liveStreaming || openJobIds.length === 0) {
      return;
    }

    let cancelled = false;
    const refreshFeeds = async () => {
      try {
        const results = await Promise.all(openJobIds.map(async (jobId) => [jobId, await fetchFieldJobLogs(jobId)] as const));
        if (!cancelled) {
          setFeeds((current) => {
            const next = { ...current };
            for (const [jobId, feed] of results) {
              next[jobId] = feed;
            }
            return next;
          });
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(error instanceof Error ? error.message : String(error));
        }
      }
    };

    refreshFeeds();
    const timer = window.setInterval(refreshFeeds, LOG_REFRESH_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [liveStreaming, openJobIds]);

  async function toggleLogs(jobId: string) {
    if (openJobIds.includes(jobId)) {
      setOpenJobIds((current) => current.filter((item) => item !== jobId));
      return;
    }

    try {
      const feed = await fetchFieldJobLogs(jobId);
      setFeeds((current) => ({ ...current, [jobId]: feed }));
      setOpenJobIds((current) => [...current, jobId]);
      setErrorMessage(null);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    }
  }

  async function refreshLogs(jobId: string) {
    try {
      const feed = await fetchFieldJobLogs(jobId);
      setFeeds((current) => ({ ...current, [jobId]: feed }));
      setErrorMessage(null);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    }
  }

  return (
    <div className="sectionStack">
      <div className="jobLogToolbar">
        <label className="jobLogToggle">
          <input
            type="checkbox"
            checked={liveStreaming}
            onChange={(event) => setLiveStreaming(event.target.checked)}
          />
          <span>Live log streaming</span>
        </label>
        <span className="muted">
          {liveStreaming ? "Open feeds refresh automatically every few seconds." : "Feeds stay paused until you open or refresh them."}
        </span>
      </div>

      {errorMessage ? <p className="jobLogError">{errorMessage}</p> : null}

      <div className="tableWrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Chapter</th>
              <th>Field</th>
              <th>Status</th>
              <th>Queue</th>
              <th>Attempts</th>
              <th>Worker</th>
              <th>Scheduled</th>
              <th>Last Error</th>
              <th>Logs</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => {
              const isOpen = openJobIds.includes(job.id);
              return (
                <tr key={job.id}>
                  <td>{job.id.slice(0, 8)}</td>
                  <td>{job.chapterSlug}</td>
                  <td>{job.fieldName}</td>
                  <td><StatusPill status={job.status} /></td>
                  <td>
                    <span>{job.queueState}</span>
                    {job.blockedReason ? <><br /><span className="muted">{job.blockedReason}</span></> : null}
                  </td>
                  <td>{job.attempts}/{job.maxAttempts}</td>
                  <td>{job.claimedBy ?? <span className="muted">unclaimed</span>}</td>
                  <td>{formatTimestamp(job.scheduledAt)}</td>
                  <td>{job.lastError ?? <span className="muted">none</span>}</td>
                  <td>
                    <div className="buttonRow">
                      <button type="button" className="buttonSecondary" onClick={() => toggleLogs(job.id)}>
                        {isOpen ? "Hide logs" : "View logs"}
                      </button>
                      {isOpen ? (
                        <button type="button" className="buttonSecondary" onClick={() => refreshLogs(job.id)}>
                          Refresh
                        </button>
                      ) : null}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {openJobIds.length > 0 ? (
        <div className="jobLogGrid">
          {openJobIds.map((jobId) => {
            const job = jobs.find((item) => item.id === jobId);
            const feed = feeds[jobId];
            return (
              <section key={jobId} className="jobLogPanel">
                <div className="jobLogPanelHeader">
                  <div>
                    <p className="eyebrow">Job Feed</p>
                    <h3>{job?.chapterSlug ?? jobId.slice(0, 8)} · {job?.fieldName ?? "field-job"}</h3>
                  </div>
                  <div className="jobLogPanelMeta">
                    <span>{job?.status ?? "unknown"}</span>
                    <span>{feed ? `${feed.lines.length} lines` : "loading"}</span>
                    <span>{feed ? `${feed.dedupedCount} deduped` : ""}</span>
                  </div>
                </div>
                <pre className="jobLogTerminal">
                  {feed?.lines.length ? (
                    feed.lines.map((line) => `[${formatTimestamp(line.createdAt)}] ${line.attempt !== null ? `attempt ${line.attempt} | ` : ""}${line.message}`).join("\n")
                  ) : (
                    "Waiting for log lines..."
                  )}
                </pre>
              </section>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
