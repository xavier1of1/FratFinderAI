"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

import { buildCampaignReport } from "@/lib/campaign-report";
import { MetricCard } from "@/components/metric-card";
import { ProgressMeter } from "@/components/progress-meter";
import { StatusPill } from "@/components/status-pill";
import { computeChapterSearchComparison, computeRuntimeComparison } from "@/lib/runtime-comparison";
import type { AdaptiveInsights, CampaignRun, CampaignRunConfig, CrawlRunListItem } from "@/lib/types";
import { buildDefaultV4ProgramConfig } from "@/lib/v4-program";

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

function computeCampaignQueueEfficiency(summary: CampaignRun["summary"] | null | undefined): {
  totalEvents: number;
  requeueRate: number;
  terminalRate: number;
  burnDown: number;
} {
  const totalProcessed = Number(summary?.totalProcessed ?? 0);
  const totalRequeued = Number(summary?.totalRequeued ?? 0);
  const totalFailedTerminal = Number(summary?.totalFailedTerminal ?? 0);
  const totalEvents = Math.max(totalProcessed + totalRequeued + totalFailedTerminal, 1);
  return {
    totalEvents,
    requeueRate: totalRequeued / totalEvents,
    terminalRate: totalFailedTerminal / totalEvents,
    burnDown: Math.max(0, Math.abs(Number(summary?.queueDepthDelta ?? 0))),
  };
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

async function fetchCampaignDetail(id: string): Promise<CampaignRun> {
  const response = await fetch(`/api/campaign-runs/${id}`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<CampaignRun>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch campaign detail: ${response.status}`);
  }
  return payload.data;
}

async function fetchCrawlRuns(): Promise<CrawlRunListItem[]> {
  const response = await fetch("/api/runs?limit=800", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<CrawlRunListItem[]>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch crawl runs: ${response.status}`);
  }
  return payload.data;
}

async function fetchAdaptiveInsights(sourceSlugs: string[]): Promise<AdaptiveInsights> {
  const query = new URLSearchParams();
  if (sourceSlugs.length > 0) {
    query.set("sourceSlugs", sourceSlugs.join(","));
  }
  query.set("windowDays", "14");
  query.set("limit", "25");

  const response = await fetch(`/api/adaptive/insights?${query.toString()}`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<AdaptiveInsights>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch adaptive insights: ${response.status}`);
  }
  return payload.data;
}

function extractCampaignSourceSlugs(campaign: CampaignRun): string[] {
  const sourceSlugs = new Set<string>();

  for (const event of campaign.events) {
    const payload = event.payload as Record<string, unknown>;
    const sourceSlug = payload.sourceSlug;
    if (typeof sourceSlug === "string" && sourceSlug.trim()) {
      sourceSlugs.add(sourceSlug.trim());
    }
  }

  if (sourceSlugs.size === 0) {
    for (const item of campaign.items) {
      if (item.fraternitySlug?.trim()) {
        sourceSlugs.add(`${item.fraternitySlug.trim()}-main`);
      }
    }
  }

  return [...sourceSlugs];
}

export function CampaignsDashboard({
  initialCampaigns,
  initialRuns
}: {
  initialCampaigns: CampaignRun[];
  initialRuns: CrawlRunListItem[];
}) {
  const [campaigns, setCampaigns] = useState<CampaignRun[]>(sortCampaigns(initialCampaigns));
  const [crawlRuns, setCrawlRuns] = useState<CrawlRunListItem[]>(initialRuns);
  const [selectedId, setSelectedId] = useState<string | null>(initialCampaigns[0]?.id ?? null);
  const [selectedCampaignDetail, setSelectedCampaignDetail] = useState<CampaignRun | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [adaptiveInsights, setAdaptiveInsights] = useState<AdaptiveInsights | null>(null);
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

  const selectedCampaignSummary = useMemo(() => {
    if (!campaigns.length) {
      return null;
    }
    if (!selectedId) {
      return campaigns[0] ?? null;
    }
    return campaigns.find((item) => item.id === selectedId) ?? campaigns[0] ?? null;
  }, [campaigns, selectedId]);

  const selectedCampaign = useMemo(() => {
    if (!selectedCampaignSummary) {
      return null;
    }
    if (selectedCampaignDetail && selectedCampaignDetail.id === selectedCampaignSummary.id) {
      return selectedCampaignDetail;
    }
    return selectedCampaignSummary;
  }, [selectedCampaignDetail, selectedCampaignSummary]);

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
      const [campaignData, runData] = await Promise.all([fetchCampaigns(), fetchCrawlRuns()]);
      setCampaigns(campaignData);
      setCrawlRuns(runData);
      const selectedStillExists = selectedId ? campaignData.some((item) => item.id === selectedId) : false;
      let nextSelectedId = selectedId;
      if (options?.selectNewest && campaignData[0]) {
        nextSelectedId = campaignData[0].id;
        setSelectedId(campaignData[0].id);
      } else if (!selectedStillExists) {
        nextSelectedId = campaignData[0]?.id ?? null;
        setSelectedId(campaignData[0]?.id ?? null);
      }
      if (nextSelectedId) {
        const detail = await fetchCampaignDetail(nextSelectedId);
        setSelectedCampaignDetail(detail);
      } else {
        setSelectedCampaignDetail(null);
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

  useEffect(() => {
    if (!selectedCampaignSummary) {
      setSelectedCampaignDetail(null);
      return;
    }
    void fetchCampaignDetail(selectedCampaignSummary.id)
      .then((detail) => setSelectedCampaignDetail(detail))
      .catch((error) => setErrorMessage(error instanceof Error ? error.message : String(error)));
  }, [selectedCampaignSummary?.id]);

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

  async function launchV4Program() {
    setIsSubmitting(true);
    setErrorMessage(null);
    try {
      const response = await fetch("/api/campaign-runs", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          config: buildDefaultV4ProgramConfig()
        })
      });
      const result = (await response.json()) as ApiEnvelope<CampaignRun>;
      if (!response.ok || !result.success) {
        if (!result.success) {
          throw new Error(`${result.error.code}: ${result.error.message}`);
        }
        throw new Error(`Failed to launch V4 program (${response.status})`);
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
  const campaignSourceSlugs = useMemo(() => (selectedCampaign ? extractCampaignSourceSlugs(selectedCampaign) : []), [selectedCampaign]);
  const runtimeComparison = useMemo(
    () =>
      computeRuntimeComparison(crawlRuns, {
        sourceSlugs: campaignSourceSlugs
      }),
    [crawlRuns, campaignSourceSlugs]
  );
  const chapterSearchComparison = useMemo(
    () =>
      computeChapterSearchComparison(crawlRuns, {
        sourceSlugs: campaignSourceSlugs
      }),
    [crawlRuns, campaignSourceSlugs]
  );
  const contactQueueEfficiency = useMemo(() => computeCampaignQueueEfficiency(displaySummary), [displaySummary]);
  const lowConfidenceDrift = useMemo(() => {
    const driftRows = selectedCampaign?.telemetry.reviewReasonDrift ?? [];
    return driftRows.reduce(
      (accumulator, row) => {
        const reason = row.reason.toLowerCase();
        const delta = Number(row.delta ?? 0);
        if (reason.includes("low-confidence") || reason.includes("low confidence")) {
          accumulator.lowConfidence += delta;
        }
        if (reason.includes("contact_email")) {
          accumulator.email += delta;
        }
        if (reason.includes("website_url")) {
          accumulator.website += delta;
        }
        return accumulator;
      },
      { lowConfidence: 0, email: 0, website: 0 }
    );
  }, [selectedCampaign?.telemetry.reviewReasonDrift]);

  useEffect(() => {
    if (!selectedCampaign) {
      setAdaptiveInsights(null);
      return;
    }
    void fetchAdaptiveInsights(campaignSourceSlugs)
      .then((data) => setAdaptiveInsights(data))
      .catch((error) => setErrorMessage(error instanceof Error ? error.message : String(error)));
  }, [selectedCampaign?.id, campaignSourceSlugs.join(",")]);

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
            <button type="button" className="buttonSecondary" disabled={isSubmitting} onClick={() => void launchV4Program()}>
              {isSubmitting ? "Launching..." : "Launch V4 RL Program"}
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

              {selectedCampaign.config.programMode === "v4_rl_improvement" ? (
                <>
                  <h3>V4 Program Status</h3>
                  <div className="metrics">
                    <MetricCard label="Program Phase" value={selectedCampaign.telemetry.programPhase ?? "n/a"} />
                    <MetricCard label="Policy Version" value={selectedCampaign.telemetry.activePolicyVersion ?? "n/a"} />
                    <MetricCard label="Policy Snapshot" value={selectedCampaign.telemetry.activePolicySnapshotId ?? "n/a"} />
                    <MetricCard
                      label="Delayed Rewards"
                      value={formatNumber(selectedCampaign.telemetry.delayedRewardHealth?.delayedRewardEventCount)}
                    />
                    <MetricCard
                      label="Placeholder Reviews"
                      value={formatNumber(selectedCampaign.telemetry.delayedRewardHealth?.placeholderReviewCount)}
                    />
                    <MetricCard
                      label="Overlong Reviews"
                      value={formatNumber(selectedCampaign.telemetry.delayedRewardHealth?.overlongReviewCount)}
                    />
                  </div>
                  {selectedCampaign.telemetry.queueStallAlert?.active ? (
                    <div className="warningBanner">
                      <strong>Queue stall alert.</strong> No meaningful processed-job movement has been observed since{" "}
                      <code>{formatTimestamp(selectedCampaign.telemetry.queueStallAlert.since)}</code> while queued backlog remains at{" "}
                      <code>{formatNumber(selectedCampaign.telemetry.queueStallAlert.queuedDepth)}</code>.
                    </div>
                  ) : null}
                  {selectedCampaign.telemetry.acceptanceGate ? (
                    <div className="tableWrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Acceptance Gate</th>
                            <th>Value</th>
                            <th>Target</th>
                            <th>Passed</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedCampaign.telemetry.acceptanceGate.checks.map((check) => (
                            <tr key={check.label}>
                              <td>{check.label}</td>
                              <td>{check.value}</td>
                              <td>{check.target}</td>
                              <td>{check.passed ? "yes" : "no"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}
                </>
              ) : null}

              <h3>Contact Resolution Slice</h3>
              <div className="comparisonGrid">
                <div className="comparisonCard">
                  <div className="benchmarkListItemHeader">
                    <strong>Queue Efficiency</strong>
                    <span className="cellHint">campaign aggregate</span>
                  </div>
                  <div className="comparisonMetrics">
                    <div>
                      <span className="comparisonLabel">Processed</span>
                      <strong>{formatNumber(displaySummary?.totalProcessed)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Requeued</span>
                      <strong>{formatNumber(displaySummary?.totalRequeued)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Terminal</span>
                      <strong>{formatNumber(displaySummary?.totalFailedTerminal)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Burn Down</span>
                      <strong>{formatNumber(contactQueueEfficiency.burnDown)}</strong>
                    </div>
                  </div>
                </div>
                <div className="comparisonCard">
                  <div className="benchmarkListItemHeader">
                    <strong>Low-Confidence Drift</strong>
                    <span className="cellHint">review reason changes</span>
                  </div>
                  <div className="comparisonMetrics">
                    <div>
                      <span className="comparisonLabel">Low-Confidence Delta</span>
                      <strong>{formatNumber(lowConfidenceDrift.lowConfidence)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Email Delta</span>
                      <strong>{formatNumber(lowConfidenceDrift.email)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Website Delta</span>
                      <strong>{formatNumber(lowConfidenceDrift.website)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Requeue Rate</span>
                      <strong>{formatPercent(contactQueueEfficiency.requeueRate)}</strong>
                    </div>
                  </div>
                </div>
              </div>
              <div className="tableWrap">
                <table>
                  <thead>
                    <tr>
                      <th>Gate</th>
                      <th>Value</th>
                      <th>Target</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>Requeue Clamp</td>
                      <td>{formatPercent(contactQueueEfficiency.requeueRate)}</td>
                      <td>&lt;= 35% queue churn</td>
                      <td><strong>{contactQueueEfficiency.requeueRate <= 0.35 ? "pass" : "fail"}</strong></td>
                    </tr>
                    <tr>
                      <td>Actionable Burn</td>
                      <td>{formatNumber(contactQueueEfficiency.burnDown)}</td>
                      <td>&gt; 0 queued jobs burned</td>
                      <td><strong>{contactQueueEfficiency.burnDown > 0 ? "pass" : "fail"}</strong></td>
                    </tr>
                    <tr>
                      <td>Terminal Rate</td>
                      <td>{formatPercent(contactQueueEfficiency.terminalRate)}</td>
                      <td>&lt;= 25% terminal exits</td>
                      <td><strong>{contactQueueEfficiency.terminalRate <= 0.25 ? "pass" : "fail"}</strong></td>
                    </tr>
                    <tr>
                      <td>Low-Confidence Drift</td>
                      <td>{formatNumber(lowConfidenceDrift.lowConfidence)}</td>
                      <td>&lt;= 0 net increase</td>
                      <td><strong>{lowConfidenceDrift.lowConfidence <= 0 ? "pass" : "fail"}</strong></td>
                    </tr>
                  </tbody>
                </table>
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
                              title={`${new Date(point.timestamp).toLocaleTimeString()} - ${(point.successRate * 100).toFixed(1)}%`}
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
                              title={`${point.label} - ${point.jobsPerMinute.toFixed(1)} jobs/min`}
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

              <h3>Legacy vs Adaptive Runtime</h3>
              {runtimeComparison.totalRuns === 0 ? (
                <p className="muted">No crawl runs found for this campaign scope yet.</p>
              ) : (
                <div className="comparisonGrid">
                  <div className="comparisonCard">
                    <div className="benchmarkListItemHeader">
                      <strong>Legacy ({runtimeComparison.scopeLabel})</strong>
                      <span className="cellHint">{runtimeComparison.legacy.runCount} runs</span>
                    </div>
                    <div className="comparisonMetrics">
                      <div>
                        <span className="comparisonLabel">Success</span>
                        <strong>{formatPercent(runtimeComparison.legacy.successRate)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Avg Seen</span>
                        <strong>{formatNumber(runtimeComparison.legacy.avgRecordsSeen, 1)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Avg Upserted</span>
                        <strong>{formatNumber(runtimeComparison.legacy.avgRecordsUpserted, 1)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Avg Sessions</span>
                        <strong>{formatNumber(runtimeComparison.legacy.avgCrawlSessions, 2)}</strong>
                      </div>
                    </div>
                  </div>
                  <div className="comparisonCard">
                    <div className="benchmarkListItemHeader">
                      <strong>Adaptive ({runtimeComparison.scopeLabel})</strong>
                      <span className="cellHint">{runtimeComparison.adaptive.runCount} runs</span>
                    </div>
                    <div className="comparisonMetrics">
                      <div>
                        <span className="comparisonLabel">Success</span>
                        <strong>{formatPercent(runtimeComparison.adaptive.successRate)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Avg Seen</span>
                        <strong>{formatNumber(runtimeComparison.adaptive.avgRecordsSeen, 1)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Avg Upserted</span>
                        <strong>{formatNumber(runtimeComparison.adaptive.avgRecordsUpserted, 1)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Avg Sessions</span>
                        <strong>{formatNumber(runtimeComparison.adaptive.avgCrawlSessions, 2)}</strong>
                      </div>
                    </div>
                  </div>
                  <div className="comparisonCard">
                    <div className="benchmarkListItemHeader">
                      <strong>Adaptive Delta</strong>
                      <span className="cellHint">adaptive - legacy</span>
                    </div>
                    <div className="comparisonMetrics">
                      <div>
                        <span className="comparisonLabel">Success</span>
                        <strong>{formatPercent(runtimeComparison.deltas.successRate)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Avg Seen</span>
                        <strong>{formatNumber(runtimeComparison.deltas.avgRecordsSeen, 1)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">Records/Page</span>
                        <strong>{formatNumber(runtimeComparison.deltas.avgRecordsPerPage, 2)}</strong>
                      </div>
                      <div>
                        <span className="comparisonLabel">LLM Calls</span>
                        <strong>{formatNumber(runtimeComparison.deltas.avgLlmCalls, 2)}</strong>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              <h3>Chapter Search Gates</h3>
              {chapterSearchComparison.totalRuns === 0 ? (
                <p className="muted">No chapter-search telemetry found for this campaign scope yet.</p>
              ) : (
                <>
                  <div className="comparisonGrid">
                    <div className="comparisonCard">
                      <div className="benchmarkListItemHeader">
                        <strong>Adaptive Chapter Search</strong>
                        <span className="cellHint">{chapterSearchComparison.adaptive.runCount} runs</span>
                      </div>
                      <div className="comparisonMetrics">
                        <div>
                          <span className="comparisonLabel">Canonical</span>
                          <strong>{formatNumber(chapterSearchComparison.adaptive.avgCanonicalCreated, 1)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Provisional</span>
                          <strong>{formatNumber(chapterSearchComparison.adaptive.avgProvisionalCreated, 1)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Skipped Sites</span>
                          <strong>{formatNumber(chapterSearchComparison.adaptive.avgChapterOwnedSkipped, 1)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Wall Time</span>
                          <strong>{formatDuration(chapterSearchComparison.adaptive.avgWallTimeMs)}</strong>
                        </div>
                      </div>
                    </div>
                    <div className="comparisonCard">
                      <div className="benchmarkListItemHeader">
                        <strong>Gate Status</strong>
                        <span className="cellHint">chapter search only</span>
                      </div>
                      <div className="comparisonMetrics">
                        <div>
                          <span className="comparisonLabel">Skipped Fanout</span>
                          <strong>{formatNumber(chapterSearchComparison.deltas.avgChapterOwnedSkipped, 1)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Institutional</span>
                          <strong>{formatNumber(chapterSearchComparison.adaptive.avgInstitutionalFollowed, 1)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Broader Web</span>
                          <strong>{formatNumber(chapterSearchComparison.adaptive.avgBroaderWebFollowed, 1)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Rejected</span>
                          <strong>{formatNumber(chapterSearchComparison.adaptive.avgRejected, 1)}</strong>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="tableWrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Gate</th>
                          <th>Value</th>
                          <th>Target</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {chapterSearchComparison.gates.map((gate) => (
                          <tr key={gate.label}>
                            <td>{gate.label}</td>
                            <td>{gate.value}</td>
                            <td>{gate.target}</td>
                            <td><strong>{gate.passed ? "pass" : "fail"}</strong></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}

              <h3>Adaptive Attribution Insights</h3>
              {adaptiveInsights ? (
                <>
                  <div className="comparisonGrid">
                    <div className="comparisonCard">
                      <div className="benchmarkListItemHeader">
                        <strong>Guardrail Hit Rate</strong>
                        <span className="cellHint">adaptive pages only</span>
                      </div>
                      <div className="comparisonMetrics">
                        <div>
                          <span className="comparisonLabel">Hit Rate</span>
                          <strong>{formatPercent(adaptiveInsights.guardrailHitRate)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Guardrail Pages</span>
                          <strong>{formatNumber(adaptiveInsights.guardrailPages)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Valid Missing</span>
                          <strong>{formatNumber(adaptiveInsights.validMissingCount)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Verified Websites</span>
                          <strong>{formatNumber(adaptiveInsights.verifiedWebsiteCount)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Delayed Rewards</span>
                          <strong>{formatNumber(adaptiveInsights.delayedRewardEventCount)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Placeholder Reviews</span>
                          <strong>{formatNumber(adaptiveInsights.placeholderReviewCount)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Overlong Reviews</span>
                          <strong>{formatNumber(adaptiveInsights.overlongReviewCount)}</strong>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="tableWrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Action Family</th>
                          <th>Count</th>
                          <th>Avg Score</th>
                          <th>Avg Risk</th>
                          <th>Records Extracted</th>
                        </tr>
                      </thead>
                      <tbody>
                        {adaptiveInsights.actionLeaderboard.slice(0, 8).map((row) => (
                          <tr key={row.actionType}>
                            <td>{row.actionType}</td>
                            <td>{formatNumber(row.count)}</td>
                            <td>{formatNumber(row.avgScore, 3)}</td>
                            <td>{formatNumber(row.avgRisk, 3)}</td>
                            <td>{formatNumber(row.recordsExtracted)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <div className="tableWrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Delayed Credit Action</th>
                          <th>Count</th>
                          <th>Avg Reward</th>
                          <th>Total Reward</th>
                        </tr>
                      </thead>
                      <tbody>
                        {adaptiveInsights.delayedAttribution.slice(0, 8).map((row) => (
                          <tr key={row.actionType}>
                            <td>{row.actionType}</td>
                            <td>{formatNumber(row.count)}</td>
                            <td>{formatNumber(row.avgReward, 3)}</td>
                            <td>{formatNumber(row.totalReward, 3)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              ) : (
                <p className="muted">Adaptive insights are loading for this campaign scope.</p>
              )}

              {selectedCampaign.config.programMode === "v4_rl_improvement" ? (
                <>
                  <h3>Promotion Decisions</h3>
                  {selectedCampaign.telemetry.promotionDecisions?.length ? (
                    <div className="tableWrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Round</th>
                            <th>Policy</th>
                            <th>Snapshot</th>
                            <th>Balanced Score</th>
                            <th>Queued</th>
                            <th>Promoted</th>
                            <th>Reason</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedCampaign.telemetry.promotionDecisions.map((decision) => (
                            <tr key={`${decision.round}-${decision.stagedPolicyVersion}`}>
                              <td>{decision.round}</td>
                              <td>{decision.stagedPolicyVersion}</td>
                              <td>{decision.snapshotId ?? "n/a"}</td>
                              <td>{formatNumber(decision.balancedScore, 4)}</td>
                              <td>{formatNumber(decision.queueQueued)}</td>
                              <td>{decision.promoted ? "yes" : "no"}</td>
                              <td>{decision.reason}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="muted">No promotion decisions recorded yet.</p>
                  )}

                  <h3>Review Drift</h3>
                  {selectedCampaign.telemetry.reviewReasonDrift?.length ? (
                    <div className="tableWrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Reason</th>
                            <th>Baseline</th>
                            <th>Latest</th>
                            <th>Delta</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedCampaign.telemetry.reviewReasonDrift.slice(0, 10).map((row) => (
                            <tr key={row.reason}>
                              <td>{row.reason}</td>
                              <td>{formatNumber(row.baselineCount)}</td>
                              <td>{formatNumber(row.latestCount)}</td>
                              <td>{formatNumber(row.delta)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="muted">No review drift recorded yet.</p>
                  )}
                </>
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

















