"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

import { buildCampaignReport } from "@/lib/campaign-report";
import { MetricCard } from "@/components/metric-card";
import { ProgressMeter } from "@/components/progress-meter";
import { StatusPill } from "@/components/status-pill";
import type { CampaignRun, CampaignRunConfig } from "@/lib/types";

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

type CampaignFormState = {
  name: string;
  targetCount: number;
  controlCount: number;
  activeConcurrency: number;
  maxDurationMinutes: number;
  checkpointIntervalMs: number;
  tuningIntervalMs: number;
  itemPollIntervalMs: number;
  preflightRequired: boolean;
  autoTuningEnabled: boolean;
  controlFraternitySlugs: string;
};

function sortCampaigns(items: CampaignRun[]): CampaignRun[] {
  return [...items].sort((left, right) => new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime());
}

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "n/a";
  }
  return new Date(value).toLocaleString();
}

function formatDuration(ms: number | null | undefined): string {
  if (!ms || ms <= 0) {
    return "0s";
  }
  const totalSeconds = Math.round(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "0%";
  }
  return `${(value * 100).toFixed(1)}%`;
}

function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "0";
  }
  return value.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

async function fetchCampaigns(): Promise<CampaignRun[]> {
  const response = await fetch("/api/campaign-runs?limit=100", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<CampaignRun[]>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch campaigns: ${response.status}`);
  }
  return sortCampaigns(payload.data);
}

export function CampaignsDashboard({ initialCampaigns }: { initialCampaigns: CampaignRun[] }) {
  const [campaigns, setCampaigns] = useState<CampaignRun[]>(sortCampaigns(initialCampaigns));
  const [selectedId, setSelectedId] = useState<string | null>(initialCampaigns[0]?.id ?? null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [form, setForm] = useState<CampaignFormState>({
    name: "",
    targetCount: 20,
    controlCount: 2,
    activeConcurrency: 4,
    maxDurationMinutes: 120,
    checkpointIntervalMs: 300000,
    tuningIntervalMs: 900000,
    itemPollIntervalMs: 15000,
    preflightRequired: true,
    autoTuningEnabled: true,
    controlFraternitySlugs: ""
  });

  const selectedCampaign = useMemo(() => {
    if (!campaigns.length) {
      return null;
    }
    if (!selectedId) {
      return campaigns[0] ?? null;
    }
    return campaigns.find((item) => item.id === selectedId) ?? campaigns[0] ?? null;
  }, [campaigns, selectedId]);

  const activeCount = useMemo(
    () => campaigns.filter((item) => item.status === "queued" || item.status === "running").length,
    [campaigns]
  );
  const runtimeDrift = useMemo(() => {
    if (!selectedCampaign) {
      return false;
    }
    return selectedCampaign.status === "running" && selectedCampaign.runtimeActive === false;
  }, [selectedCampaign]);

  async function refreshCampaigns(options?: { selectNewest?: boolean }) {
    setIsRefreshing(true);
    try {
      const data = await fetchCampaigns();
      setCampaigns(data);
      const selectedStillExists = selectedId ? data.some((item) => item.id === selectedId) : false;
      if (options?.selectNewest && data[0]) {
        setSelectedId(data[0].id);
      } else if (!selectedStillExists) {
        setSelectedId(data[0]?.id ?? null);
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsRefreshing(false);
    }
  }

  useEffect(() => {
    if (activeCount === 0) {
      return;
    }
    const interval = setInterval(() => {
      void refreshCampaigns();
    }, 5000);
    return () => clearInterval(interval);
  }, [activeCount]);

  async function handleCreateCampaign(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSubmitting(true);
    setErrorMessage(null);

    try {
      const payload: { name?: string; config: Partial<CampaignRunConfig> } = {
        name: form.name.trim() || undefined,
        config: {
          targetCount: form.targetCount,
          controlCount: form.controlCount,
          activeConcurrency: form.activeConcurrency,
          maxDurationMinutes: form.maxDurationMinutes,
          checkpointIntervalMs: form.checkpointIntervalMs,
          tuningIntervalMs: form.tuningIntervalMs,
          itemPollIntervalMs: form.itemPollIntervalMs,
          preflightRequired: form.preflightRequired,
          autoTuningEnabled: form.autoTuningEnabled,
          controlFraternitySlugs: form.controlFraternitySlugs
            .split(",")
            .map((item) => item.trim())
            .filter(Boolean)
        }
      };

      const response = await fetch("/api/campaign-runs", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });
      const result = (await response.json()) as ApiEnvelope<CampaignRun>;
      if (!response.ok || !result.success) {
        if (!result.success) {
          throw new Error(`${result.error.code}: ${result.error.message}`);
        }
        throw new Error(`Failed to create campaign (${response.status})`);
      }

      await refreshCampaigns({ selectNewest: true });
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSubmitting(false);
    }
  }

  async function sendAction(path: string) {
    setErrorMessage(null);
    try {
      const response = await fetch(path, { method: "POST" });
      const result = (await response.json()) as ApiEnvelope<unknown>;
      if (!response.ok || !result.success) {
        if (!result.success) {
          throw new Error(`${result.error.code}: ${result.error.message}`);
        }
        throw new Error(`Request failed (${response.status})`);
      }
      await refreshCampaigns();
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    }
  }

  const latestSummary = selectedCampaign?.summary;
  const bestAnyContact = useMemo(
    () => campaigns.reduce((best, item) => Math.max(best, item.summary.anyContactSuccessRate), 0),
    [campaigns]
  );
  const campaignReport = useMemo(() => (selectedCampaign ? buildCampaignReport(selectedCampaign) : null), [selectedCampaign]);
  const displaySummary = campaignReport?.summary ?? selectedCampaign?.summary ?? null;
  const checkpointSeries = useMemo(() => {
    if (!selectedCampaign) {
      return [];
    }
    return [...selectedCampaign.events]
      .filter((event) => event.eventType === "checkpoint")
      .reverse()
      .map((event) => {
        const payloadSummary = (event.payload.summary ?? {}) as Record<string, unknown>;
        return {
          label: new Date(event.createdAt).toLocaleTimeString(),
          jobsPerMinute: Number(payloadSummary.jobsPerMinute ?? 0),
          anyContactSuccessRate: Number(payloadSummary.anyContactSuccessRate ?? 0)
        };
      });
  }, [selectedCampaign]);

  return (
    <div className="sectionStack">
      <section className="panel heroPanel">
        <h2>Campaign Benchmark Control Room</h2>
        <p className="sectionDescription">
          Launch multi-fraternity campaigns, watch queue health in real time, and capture the diagnostics that tell us which crawl habits actually scale.
        </p>
        <div className="heroGrid">
          <div>
            <div className="metrics">
              <MetricCard label="Saved Campaigns" value={campaigns.length} />
              <MetricCard label="Running / Queued" value={activeCount} />
              <MetricCard label="Latest Any-Contact" value={latestSummary ? formatPercent(latestSummary.anyContactSuccessRate) : "n/a"} />
              <MetricCard label="Best Any-Contact" value={formatPercent(bestAnyContact)} />
              <MetricCard label="Latest Throughput" value={latestSummary ? `${formatNumber(latestSummary.jobsPerMinute, 1)} jobs/min` : "n/a"} />
            </div>
          </div>
          <div className="heroAsideCard">
            <p className="eyebrow">What This Tracks</p>
            <div className="heroChecklistItem">
              <strong>Resumable campaigns</strong>
              <span>Each fraternity request is linked, checkpointed, and visible as the queue moves.</span>
            </div>
            <div className="heroChecklistItem">
              <strong>Safe tuning</strong>
              <span>Provider health can throttle concurrency before search instability turns into queue churn.</span>
            </div>
            <div className="heroChecklistItem">
              <strong>Habits that work</strong>
              <span>Coverage, retries, and source-native yield are all captured as campaign scorecards.</span>
            </div>
          </div>
        </div>
        {runtimeDrift ? (
          <div className="warningBanner">
            <strong>Runner attention needed.</strong> This campaign is marked <code>running</code> in the database, but no active in-memory runner is attached right now. Use <code>Resume</code> to reattach safely.
          </div>
        ) : null}
      </section>

      <section className="panel benchmarkControls">
        <h2>Launch Campaign</h2>
        <p className="sectionDescription">Create a 20-fraternity style benchmark run that can stay live on the site for a long validation window.</p>

        <form onSubmit={handleCreateCampaign}>
          <div className="benchmarkFormGrid">
            <div className="fieldStack">
              <label htmlFor="campaign-name">Campaign Name</label>
              <input id="campaign-name" value={form.name} onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))} placeholder="Optional label" />
            </div>
            <div className="fieldStack">
              <label htmlFor="campaign-target">Target Count</label>
              <input id="campaign-target" type="number" min={3} max={50} value={form.targetCount} onChange={(event) => setForm((current) => ({ ...current, targetCount: Number(event.target.value) || 3 }))} />
            </div>
            <div className="fieldStack">
              <label htmlFor="campaign-control">Control Count</label>
              <input id="campaign-control" type="number" min={0} max={10} value={form.controlCount} onChange={(event) => setForm((current) => ({ ...current, controlCount: Number(event.target.value) || 0 }))} />
            </div>
            <div className="fieldStack">
              <label htmlFor="campaign-concurrency">Active Concurrency</label>
              <input id="campaign-concurrency" type="number" min={1} max={12} value={form.activeConcurrency} onChange={(event) => setForm((current) => ({ ...current, activeConcurrency: Number(event.target.value) || 1 }))} />
            </div>
            <div className="fieldStack">
              <label htmlFor="campaign-duration">Max Duration (min)</label>
              <input id="campaign-duration" type="number" min={15} max={240} value={form.maxDurationMinutes} onChange={(event) => setForm((current) => ({ ...current, maxDurationMinutes: Number(event.target.value) || 15 }))} />
            </div>
            <div className="fieldStack">
              <label htmlFor="campaign-checkpoint">Checkpoint (ms)</label>
              <input id="campaign-checkpoint" type="number" min={10000} max={600000} value={form.checkpointIntervalMs} onChange={(event) => setForm((current) => ({ ...current, checkpointIntervalMs: Number(event.target.value) || 10000 }))} />
            </div>
            <div className="fieldStack">
              <label htmlFor="campaign-tuning">Tuning (ms)</label>
              <input id="campaign-tuning" type="number" min={30000} max={900000} value={form.tuningIntervalMs} onChange={(event) => setForm((current) => ({ ...current, tuningIntervalMs: Number(event.target.value) || 30000 }))} />
            </div>
            <div className="fieldStack">
              <label htmlFor="campaign-poll">Poll (ms)</label>
              <input id="campaign-poll" type="number" min={5000} max={120000} value={form.itemPollIntervalMs} onChange={(event) => setForm((current) => ({ ...current, itemPollIntervalMs: Number(event.target.value) || 5000 }))} />
            </div>
            <div className="fieldStack fieldStackWide">
              <label htmlFor="campaign-controls">Explicit Control Slugs</label>
              <input id="campaign-controls" value={form.controlFraternitySlugs} onChange={(event) => setForm((current) => ({ ...current, controlFraternitySlugs: event.target.value }))} placeholder="Optional comma-separated slugs" />
            </div>
          </div>

          <div className="buttonRow buttonRowWrap">
            <label className="toggleLabel">
              <input type="checkbox" checked={form.preflightRequired} onChange={(event) => setForm((current) => ({ ...current, preflightRequired: event.target.checked }))} />
              <span>Require healthy preflight</span>
            </label>
            <label className="toggleLabel">
              <input type="checkbox" checked={form.autoTuningEnabled} onChange={(event) => setForm((current) => ({ ...current, autoTuningEnabled: event.target.checked }))} />
              <span>Enable auto-tuning</span>
            </label>
            <button type="submit" className="buttonPrimaryAuto" disabled={isSubmitting}>
              {isSubmitting ? "Launching..." : "Launch Campaign"}
            </button>
            <button type="button" className="buttonSecondary" disabled={isRefreshing} onClick={() => void refreshCampaigns()}>
              {isRefreshing ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </form>

        {errorMessage ? <p className="benchmarkError">{errorMessage}</p> : null}
      </section>

      <section className="benchmarkLayout">
        <article className="panel">
          <h2>Campaign History</h2>
          <p className="sectionDescription">Choose a campaign to inspect cohort mix, tuning actions, live request states, and final coverage.</p>
          {campaigns.length === 0 ? (
            <p className="muted">No campaigns yet. Launch one to begin collecting campaign telemetry.</p>
          ) : (
            <div className="benchmarkList">
              {campaigns.map((item) => (
                <button type="button" key={item.id} className={`benchmarkListItem${item.id === selectedCampaign?.id ? " active" : ""}`} onClick={() => setSelectedId(item.id)}>
                  <div className="benchmarkListItemHeader">
                    <strong>{item.name}</strong>
                    <StatusPill status={item.status} />
                  </div>
                  <div className="benchmarkListMeta">
                    <span>{item.items.length} fraternities</span>
                    <span>{formatTimestamp(item.createdAt)}</span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </article>

        <article className="panel">
          <h2>Campaign Details</h2>
          {selectedCampaign ? (
            <>
              <div className="benchmarkSelectedHeader">
                <div>
                  <h3>{selectedCampaign.name}</h3>
                  <p className="muted">
                    {selectedCampaign.items.length} items, {selectedCampaign.config.controlCount} controls, concurrency {selectedCampaign.telemetry.activeConcurrency ?? selectedCampaign.config.activeConcurrency}, runtime {selectedCampaign.runtimeActive ? "attached" : selectedCampaign.status === "running" ? "detached" : "idle"}
                  </p>
                </div>
                <StatusPill status={selectedCampaign.status} />
              </div>

              <div className="benchmarkMetaGrid">
                <div className="benchmarkMetaCard"><p className="benchmarkMetaLabel">Started</p><p className="benchmarkMetaValue">{formatTimestamp(selectedCampaign.startedAt)}</p></div>
                <div className="benchmarkMetaCard"><p className="benchmarkMetaLabel">Finished</p><p className="benchmarkMetaValue">{formatTimestamp(selectedCampaign.finishedAt)}</p></div>
                <div className="benchmarkMetaCard"><p className="benchmarkMetaLabel">Duration</p><p className="benchmarkMetaValue">{formatDuration(displaySummary?.durationMs)}</p></div>
                <div className="benchmarkMetaCard"><p className="benchmarkMetaLabel">Throughput</p><p className="benchmarkMetaValue">{formatNumber(displaySummary?.jobsPerMinute, 1)} jobs/min</p></div>
              </div>

              <div className="progressGrid">
                <ProgressMeter label="Fraternity Completion" value={(displaySummary?.completedCount ?? 0) + (displaySummary?.failedCount ?? 0) + (displaySummary?.skippedCount ?? 0)} total={Math.max(displaySummary?.itemCount ?? 1, 1)} hint={`${displaySummary?.activeCount ?? 0} active`} />
                <ProgressMeter label="Any Contact Coverage" value={(displaySummary?.anyContactSuccessRate ?? 0) * 100} total={100} hint={formatPercent(displaySummary?.anyContactSuccessRate)} />
                <ProgressMeter label="All Three Fields" value={(displaySummary?.allThreeSuccessRate ?? 0) * 100} total={100} hint={formatPercent(displaySummary?.allThreeSuccessRate)} />
              </div>

              <div className="metrics">
                <MetricCard label="Website Coverage" value={formatPercent(displaySummary?.websiteCoverageRate)} />
                <MetricCard label="Email Coverage" value={formatPercent(displaySummary?.emailCoverageRate)} />
                <MetricCard label="Instagram Coverage" value={formatPercent(displaySummary?.instagramCoverageRate)} />
                <MetricCard label="Queue Delta" value={formatNumber(displaySummary?.queueDepthDelta)} />
                <MetricCard label="Processed Jobs" value={formatNumber(displaySummary?.totalProcessed)} />
              </div>

              <div className="buttonRow buttonRowWrap">
                <button type="button" className="buttonSecondary" onClick={() => void sendAction(`/api/campaign-runs/${selectedCampaign.id}/resume`)} disabled={selectedCampaign.status === "running"}>
                  Resume
                </button>
                <button type="button" className="buttonSecondary buttonDanger" onClick={() => void sendAction(`/api/campaign-runs/${selectedCampaign.id}/cancel`)} disabled={selectedCampaign.status === "canceled" || selectedCampaign.status === "succeeded"}>
                  Cancel
                </button>
                <a className="buttonSecondary" href={`/api/campaign-runs/${selectedCampaign.id}/export?format=json`}>
                  Export JSON
                </a>
                <a className="buttonSecondary" href={`/api/campaign-runs/${selectedCampaign.id}/export?format=csv`}>
                  Export CSV
                </a>
              </div>

              <h3>Provider Health</h3>
              <div className="tableWrap">
                <table>
                  <thead>
                    <tr>
                      <th>Health</th>
                      <th>Success Rate</th>
                      <th>Probes</th>
                      <th>Successes</th>
                      <th>Last Tune</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>{selectedCampaign.telemetry.providerHealth?.healthy ? "Healthy" : "Degraded"}</td>
                      <td>{formatPercent(selectedCampaign.telemetry.providerHealth?.successRate)}</td>
                      <td>{formatNumber(selectedCampaign.telemetry.providerHealth?.probes)}</td>
                      <td>{formatNumber(selectedCampaign.telemetry.providerHealth?.successes)}</td>
                      <td>{formatTimestamp(selectedCampaign.telemetry.lastTuneAt ?? null)}</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <div className="chartPanelGrid">
                <section className="panelInset">
                  <div className="benchmarkListItemHeader">
                    <strong>Provider Health History</strong>
                    <span className="cellHint">{selectedCampaign.telemetry.providerHealthHistory?.length ?? 0} points</span>
                  </div>
                  {selectedCampaign.telemetry.providerHealthHistory?.length ? (
                    <div className="historyChart">
                      {selectedCampaign.telemetry.providerHealthHistory.map((point) => (
                        <div key={`${point.timestamp}-${point.activeConcurrency}`} className="historyBarWrap">
                          <div className="historyBarTrack">
                            <div
                              className={`historyBar ${point.healthy ? "healthy" : "degraded"}`}
                              style={{ height: `${Math.max(10, point.successRate * 100)}%` }}
                              title={`${new Date(point.timestamp).toLocaleTimeString()} · ${(point.successRate * 100).toFixed(1)}%`}
                            />
                          </div>
                          <span className="historyBarLabel">{new Date(point.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No provider-health history yet. Preflight and tuning points will appear here.</p>
                  )}
                </section>

                <section className="panelInset">
                  <div className="benchmarkListItemHeader">
                    <strong>Checkpoint Throughput</strong>
                    <span className="cellHint">{checkpointSeries.length} checkpoints</span>
                  </div>
                  {checkpointSeries.length ? (
                    <div className="historyChart">
                      {checkpointSeries.map((point) => (
                        <div key={point.label} className="historyBarWrap">
                          <div className="historyBarTrack">
                            <div
                              className="historyBar"
                              style={{ height: `${Math.max(10, Math.min(100, point.jobsPerMinute * 10))}%` }}
                              title={`${point.label} · ${point.jobsPerMinute.toFixed(1)} jobs/min`}
                            />
                          </div>
                          <span className="historyBarLabel">{point.label}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No checkpoints yet. This chart fills in as the campaign runs.</p>
                  )}
                </section>
              </div>

              <h3>Control vs New</h3>
              {campaignReport ? (
                <div className="comparisonGrid">
                  {campaignReport.cohortComparison.map((cohort) => (
                    <div key={cohort.cohort} className="comparisonCard">
                      <div className="benchmarkListItemHeader">
                        <strong>{cohort.cohort === "new" ? "New Fraternities" : "Control Fraternities"}</strong>
                        <span className="cellHint">{cohort.itemCount} items</span>
                      </div>
                      <div className="comparisonMetrics">
                        <div>
                          <span className="comparisonLabel">Any Contact</span>
                          <strong>{formatPercent(cohort.anyContactSuccessRate)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">All Three</span>
                          <strong>{formatPercent(cohort.allThreeSuccessRate)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Completed</span>
                          <strong>{cohort.completedCount}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Failed</span>
                          <strong>{cohort.failedCount}</strong>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}

              <h3>Fraternity Items</h3>
              <div className="tableWrap">
                <table>
                  <thead>
                    <tr>
                      <th>Fraternity</th>
                      <th>Cohort</th>
                      <th>Status</th>
                      <th>Request</th>
                      <th>Any Contact</th>
                      <th>All Three</th>
                      <th>Processed</th>
                      <th>Requeued</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedCampaign.items.map((item) => (
                      <tr key={item.id}>
                        <td>
                          <strong>{item.fraternityName}</strong>
                          <div className="cellHint">{item.fraternitySlug}</div>
                        </td>
                        <td>{item.cohort}</td>
                        <td>{item.status}</td>
                        <td className="monoCell">{item.requestId ? item.requestId.slice(0, 8) : "n/a"}</td>
                        <td>{item.scorecard.chaptersWithAnyContact}</td>
                        <td>{item.scorecard.chaptersWithAllThree}</td>
                        <td>{item.scorecard.processedJobs}</td>
                        <td>{item.scorecard.requeuedJobs}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <h3>Recent Timeline</h3>
              {selectedCampaign.events.length === 0 ? (
                <p className="muted">No campaign events yet.</p>
              ) : (
                <div className="eventStream">
                  {selectedCampaign.events.slice(0, 12).map((event) => (
                    <div key={event.id} className="eventCard">
                      <div className="benchmarkListItemHeader">
                        <strong>{event.eventType.replaceAll("_", " ")}</strong>
                        <span className="cellHint">{formatTimestamp(event.createdAt)}</span>
                      </div>
                      <p className="muted">{event.message}</p>
                    </div>
                  ))}
                </div>
              )}

              {campaignReport ? (
                <>
                  <h3>Top Failure Modes</h3>
                  {campaignReport.topFailureReasons.length ? (
                    <div className="tableWrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Reason</th>
                            <th>Count</th>
                          </tr>
                        </thead>
                        <tbody>
                          {campaignReport.topFailureReasons.map((entry) => (
                            <tr key={entry.reason}>
                              <td>{entry.reason}</td>
                              <td>{entry.count}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="muted">No failure reasons recorded yet.</p>
                  )}

                  <h3>Successful Habits</h3>
                  <div className="comparisonGrid">
                    {campaignReport.topSuccessfulHabits.map((entry) => (
                      <div key={entry.label} className="comparisonCard">
                        <span className="comparisonLabel">{entry.label}</span>
                        <strong>{formatNumber(entry.value, 2)}</strong>
                      </div>
                    ))}
                  </div>

                  <h3>Recommendations</h3>
                  <div className="eventStream">
                    {campaignReport.recommendations.map((recommendation) => (
                      <div key={recommendation} className="eventCard">
                        <p>{recommendation}</p>
                      </div>
                    ))}
                  </div>
                </>
              ) : null}

              {selectedCampaign.lastError ? <p className="benchmarkError">{selectedCampaign.lastError}</p> : null}
            </>
          ) : (
            <p className="muted">Pick a campaign from the left to inspect details.</p>
          )}
        </article>
      </section>
    </div>
  );
}
