"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

import { MetricCard } from "@/components/metric-card";
import { ProgressMeter } from "@/components/progress-meter";
import { StatusPill } from "@/components/status-pill";
import { computeChapterSearchComparison, computeRuntimeComparison } from "@/lib/runtime-comparison";
import type {
  AdaptiveEpochMetric,
  BenchmarkFieldName,
  BenchmarkAlert,
  BenchmarkAlertSummary,
  BenchmarkGateReport,
  BenchmarkRunConfig,
  BenchmarkRunSummary,
  BenchmarkRunListItem,
  CrawlRunListItem,
  FieldJobGraphRunDetail,
  FieldJobGraphRunListItem,
} from "@/lib/types";

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

interface BenchmarkRunDetailResponse {
  run: BenchmarkRunListItem;
  baseline: BenchmarkRunListItem | null;
  gateReport: BenchmarkGateReport | null;
}

interface BenchmarkAlertScanResult {
  startedAt: string;
  finishedAt: string;
  consideredRuns: number;
  alertsCreated: number;
  alertsResolved: number;
}

interface BenchmarkAlertsResponse {
  alerts: BenchmarkAlert[];
  scanResult: BenchmarkAlertScanResult | null;
}

type BenchmarkFormState = {
  name: string;
  fieldName: BenchmarkFieldName;
  sourceSlug: string;
  workers: number;
  limitPerCycle: number;
  cycles: number;
  pauseMs: number;
  fieldJobRuntimeMode: "langgraph_primary";
  fieldJobGraphDurability: "exit" | "async" | "sync";
};

const FIELD_LABELS: Record<BenchmarkFieldName, string> = {
  find_website: "Website",
  find_email: "Email",
  find_instagram: "Instagram",
  all: "All Fields"
};

function sortBenchmarks(items: BenchmarkRunListItem[]): BenchmarkRunListItem[] {
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
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes === 0) {
    return `${seconds}s`;
  }
  return `${minutes}m ${seconds}s`;
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "0%";
  }
  return `${(value * 100).toFixed(digits)}%`;
}
function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "0";
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function computeQueueEfficiency(summary: BenchmarkRunSummary | null | undefined): {
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



async function fetchBenchmarks(): Promise<BenchmarkRunListItem[]> {
  const response = await fetch("/api/benchmarks?limit=200", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<BenchmarkRunListItem[]>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch benchmark runs: ${response.status}`);
  }

  return sortBenchmarks(payload.data);
}

async function fetchCrawlRuns(): Promise<CrawlRunListItem[]> {
  const response = await fetch("/api/runs?limit=600", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<CrawlRunListItem[]>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch crawl runs: ${response.status}`);
  }

  return payload.data;
}

async function fetchAdaptiveEpochs(): Promise<AdaptiveEpochMetric[]> {
  const response = await fetch("/api/adaptive/epochs?limit=60", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<AdaptiveEpochMetric[]>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch adaptive epochs: ${response.status}`);
  }

  return payload.data;
}

async function fetchBenchmarkDetail(id: string): Promise<BenchmarkRunDetailResponse> {
  const response = await fetch(`/api/benchmarks/${id}?includeComparisons=1`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<BenchmarkRunDetailResponse>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch benchmark detail: ${response.status}`);
  }

  return payload.data;
}

async function fetchBenchmarkAlerts(params: {
  benchmarkRunId?: string | null;
  severity?: "info" | "warning" | "critical" | "all";
  status?: "open" | "resolved" | "all";
  scan?: boolean;
}): Promise<BenchmarkAlertsResponse> {
  const query = new URLSearchParams();
  query.set("limit", "120");
  query.set("status", params.status ?? "open");
  query.set("severity", params.severity ?? "all");
  if (params.benchmarkRunId) {
    query.set("benchmarkRunId", params.benchmarkRunId);
  }
  if (params.scan) {
    query.set("scan", "1");
  }

  const response = await fetch(`/api/benchmarks/alerts?${query.toString()}`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<BenchmarkAlertsResponse>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch benchmark alerts: ${response.status}`);
  }

  return payload.data;
}

async function fetchBenchmarkCounts(): Promise<{
  total: number;
  queued: number;
  running: number;
  succeeded: number;
  failed: number;
}> {
  const response = await fetch("/api/benchmarks/summary", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<{
    total: number;
    queued: number;
    running: number;
    succeeded: number;
    failed: number;
  }>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch benchmark summary: ${response.status}`);
  }

  return payload.data;
}
async function fetchBenchmarkAlertSummary(): Promise<BenchmarkAlertSummary> {
  const response = await fetch("/api/benchmarks/alerts/summary", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<BenchmarkAlertSummary>;

  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch benchmark alert summary: ${response.status}`);
  }

  return payload.data;
}
async function fetchGraphRuns(params: {
  sourceSlug: string | null;
  fieldName: BenchmarkFieldName;
  runtimeMode: string;
}): Promise<FieldJobGraphRunListItem[]> {
  const query = new URLSearchParams();
  query.set("limit", "12");
  if (params.sourceSlug) {
    query.set("sourceSlug", params.sourceSlug);
  }
  if (params.fieldName !== "all") {
    query.set("fieldName", params.fieldName);
  }
  if (params.runtimeMode) {
    query.set("runtimeMode", params.runtimeMode);
  }

  const response = await fetch(`/api/field-jobs/graph-runs?${query.toString()}`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<FieldJobGraphRunListItem[]>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch field-job graph runs: ${response.status}`);
  }
  return payload.data;
}

async function fetchGraphRunDetail(runId: number): Promise<FieldJobGraphRunDetail> {
  const response = await fetch(`/api/field-jobs/graph-runs/${runId}?eventLimit=40&decisionLimit=40`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<FieldJobGraphRunDetail>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Failed to fetch field-job graph run detail: ${response.status}`);
  }
  return payload.data;
}

export function BenchmarksDashboard({
  initialBenchmarks,
  initialRuns,
  activeCampaignCount = 0,
  summaryCounts
}: {
  initialBenchmarks: BenchmarkRunListItem[];
  initialRuns: CrawlRunListItem[];
  activeCampaignCount?: number;
  summaryCounts: {
    total: number;
    queued: number;
    running: number;
    succeeded: number;
    failed: number;
  };
}) {
  const [benchmarks, setBenchmarks] = useState<BenchmarkRunListItem[]>(sortBenchmarks(initialBenchmarks));
  const [crawlRuns, setCrawlRuns] = useState<CrawlRunListItem[]>(initialRuns);
  const [counts, setCounts] = useState(summaryCounts);
  const [adaptiveEpochs, setAdaptiveEpochs] = useState<AdaptiveEpochMetric[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(initialBenchmarks[0]?.id ?? null);
  const [selectedDetail, setSelectedDetail] = useState<BenchmarkRunDetailResponse | null>(null);
  const [graphRuns, setGraphRuns] = useState<FieldJobGraphRunListItem[]>([]);
  const [selectedGraphRun, setSelectedGraphRun] = useState<FieldJobGraphRunDetail | null>(null);
  const [benchmarkAlerts, setBenchmarkAlerts] = useState<BenchmarkAlert[]>([]);
  const [globalAlertSummary, setGlobalAlertSummary] = useState<BenchmarkAlertSummary | null>(null);
  const [alertSeverityFilter, setAlertSeverityFilter] = useState<"info" | "warning" | "critical" | "all">("all");
  const [alertStatusFilter, setAlertStatusFilter] = useState<"open" | "resolved" | "all">("open");
  const [alertScanResult, setAlertScanResult] = useState<BenchmarkAlertScanResult | null>(null);
  const [isScanningAlerts, setIsScanningAlerts] = useState(false);
  const [isScanningAllAlerts, setIsScanningAllAlerts] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [form, setForm] = useState<BenchmarkFormState>({
    name: "",
    fieldName: "find_email",
    sourceSlug: "",
    workers: 8,
    limitPerCycle: 24,
    cycles: 6,
    pauseMs: 500,
    fieldJobRuntimeMode: "langgraph_primary",
    fieldJobGraphDurability: "sync"
  });

  const selectedBenchmark = useMemo(() => {
    if (!benchmarks.length) {
      return null;
    }

    if (!selectedId) {
      return benchmarks[0] ?? null;
    }

    return benchmarks.find((item) => item.id === selectedId) ?? benchmarks[0] ?? null;
  }, [benchmarks, selectedId]);

  const runningCount = counts.queued + counts.running;

  async function refreshBenchmarkList(options?: { selectNewest?: boolean }) {
    setIsRefreshing(true);
    try {
      const [benchmarkData, runData, epochData, alertSummary, countsData] = await Promise.all([
        fetchBenchmarks(),
        fetchCrawlRuns(),
        fetchAdaptiveEpochs(),
        fetchBenchmarkAlertSummary(),
        fetchBenchmarkCounts(),
      ]);
      setBenchmarks(benchmarkData);
      setCrawlRuns(runData);
      setAdaptiveEpochs(epochData);
      setGlobalAlertSummary(alertSummary);
      setCounts(countsData);

      const selectedStillExists = selectedId ? benchmarkData.some((item) => item.id === selectedId) : false;
      if (options?.selectNewest && benchmarkData[0]) {
        setSelectedId(benchmarkData[0].id);
      } else if (!selectedStillExists) {
        setSelectedId(benchmarkData[0]?.id ?? null);
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsRefreshing(false);
    }
  }
  useEffect(() => {
    if (runningCount === 0) {
      return;
    }

    const interval = setInterval(() => {
      void refreshBenchmarkList();
    }, 3500);

    return () => {
      clearInterval(interval);
    };
  }, [runningCount]);

  useEffect(() => {
    if (globalAlertSummary) {
      return;
    }
    void fetchBenchmarkAlertSummary()
      .then((summary) => setGlobalAlertSummary(summary))
      .catch((error) => setErrorMessage(error instanceof Error ? error.message : String(error)));
  }, [globalAlertSummary]);
  useEffect(() => {
    if (adaptiveEpochs.length > 0) {
      return;
    }
    void fetchAdaptiveEpochs()
      .then((rows) => setAdaptiveEpochs(rows))
      .catch((error) => setErrorMessage(error instanceof Error ? error.message : String(error)));
  }, [adaptiveEpochs.length]);

  useEffect(() => {
    const benchmark = selectedBenchmark;
    if (!benchmark) {
      setSelectedDetail(null);
      setGraphRuns([]);
      setSelectedGraphRun(null);
      setBenchmarkAlerts([]);
      setAlertScanResult(null);
      return;
    }

    let canceled = false;
    void (async () => {
      try {
        const [detail, runs, alertPayload] = await Promise.all([
          fetchBenchmarkDetail(benchmark.id),
          fetchGraphRuns({
            sourceSlug: benchmark.sourceSlug,
            fieldName: benchmark.fieldName,
            runtimeMode: benchmark.config.fieldJobRuntimeMode ?? "langgraph_primary",
          }),
          fetchBenchmarkAlerts({
            benchmarkRunId: benchmark.id,
            severity: alertSeverityFilter,
            status: alertStatusFilter,
          }),
        ]);

        if (canceled) {
          return;
        }

        setSelectedDetail(detail);
        setGraphRuns(runs);
        setBenchmarkAlerts(alertPayload.alerts);
        setAlertScanResult(alertPayload.scanResult ?? null);

        if (runs.length > 0) {
          const graphDetail = await fetchGraphRunDetail(runs[0]!.id);
          if (!canceled) {
            setSelectedGraphRun(graphDetail);
          }
        } else {
          setSelectedGraphRun(null);
        }
      } catch (error) {
        if (!canceled) {
          setErrorMessage(error instanceof Error ? error.message : String(error));
        }
      }
    })();

    return () => {
      canceled = true;
    };
  }, [selectedBenchmark?.id, selectedBenchmark?.updatedAt, alertSeverityFilter, alertStatusFilter]);

  async function handleScanAlerts() {
    if (!selectedBenchmark) {
      return;
    }
    setIsScanningAlerts(true);
    setErrorMessage(null);
    try {
      const [payload, summary] = await Promise.all([
        fetchBenchmarkAlerts({
          benchmarkRunId: selectedBenchmark.id,
          severity: alertSeverityFilter,
          status: alertStatusFilter,
          scan: true,
        }),
        fetchBenchmarkAlertSummary(),
      ]);
      setBenchmarkAlerts(payload.alerts);
      setGlobalAlertSummary(summary);
      setAlertScanResult(payload.scanResult ?? null);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsScanningAlerts(false);
    }
  }

  async function handleScanAllAlerts() {
    setIsScanningAllAlerts(true);
    setErrorMessage(null);
    try {
      const [alertsPayload, summary] = await Promise.all([
        fetchBenchmarkAlerts({
          severity: "all",
          status: "all",
          scan: true,
        }),
        fetchBenchmarkAlertSummary(),
      ]);

      setGlobalAlertSummary(summary);
      if (selectedBenchmark) {
        const scoped = alertsPayload.alerts.filter((alert) => alert.benchmarkRunId === selectedBenchmark.id);
        setBenchmarkAlerts(scoped);
      }
      setAlertScanResult(alertsPayload.scanResult ?? null);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsScanningAllAlerts(false);
    }
  }
  async function handleRunBenchmark(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSubmitting(true);
    setErrorMessage(null);

    try {
      const payload: Partial<BenchmarkRunConfig> & { name?: string } = {
        name: form.name.trim() || undefined,
        fieldName: form.fieldName,
        sourceSlug: form.sourceSlug.trim() || null,
        workers: form.workers,
        limitPerCycle: form.limitPerCycle,
        cycles: form.cycles,
        pauseMs: form.pauseMs,
        fieldJobRuntimeMode: form.fieldJobRuntimeMode,
        fieldJobGraphDurability: form.fieldJobGraphDurability
      };

      const response = await fetch("/api/benchmarks", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });

      const result = (await response.json()) as ApiEnvelope<BenchmarkRunListItem>;
      if (!response.ok || !result.success) {
        if (!result.success) {
          throw new Error(`${result.error.code}: ${result.error.message}`);
        }
        throw new Error(`Failed to create benchmark run (${response.status})`);
      }

      await refreshBenchmarkList({ selectNewest: true });
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSubmitting(false);
    }
  }

  const summary = selectedBenchmark?.summary;
  const bestThroughput = useMemo(() => benchmarks.reduce((best, item) => Math.max(best, item.summary?.jobsPerMinute ?? 0), 0), [benchmarks]);
  const bestQueueDelta = useMemo(() => benchmarks.reduce((best, item) => Math.min(best, item.summary?.queueDepthDelta ?? 0, best), 0), [benchmarks]);
  const runtimeComparison = useMemo(
    () =>
      computeRuntimeComparison(crawlRuns, {
        sourceSlug: selectedBenchmark?.sourceSlug ?? null
      }),
    [crawlRuns, selectedBenchmark?.sourceSlug]
  );
  const chapterSearchSummary = useMemo(() => {
    const scopedRuns = crawlRuns.filter((run) => {
      if (!run.chapterSearch) {
        return false;
      }
      if (!selectedBenchmark?.sourceSlug) {
        return true;
      }
      return run.sourceSlug === selectedBenchmark.sourceSlug;
    });

    const totals = scopedRuns.reduce(
      (accumulator, run) => {
        const chapterSearch = run.chapterSearch ?? {};
        accumulator.runCount += 1;
        accumulator.canonical += chapterSearch.canonicalChaptersCreated ?? 0;
        accumulator.provisional += chapterSearch.provisionalChaptersCreated ?? 0;
        accumulator.skipped += chapterSearch.chapterOwnedTargetsSkipped ?? 0;
        accumulator.national += chapterSearch.nationalTargetsFollowed ?? 0;
        accumulator.institutional += chapterSearch.institutionalTargetsFollowed ?? 0;
        accumulator.rejected += chapterSearch.candidatesRejected ?? 0;
        accumulator.wallTimeMs += chapterSearch.chapterSearchWallTimeMs ?? 0;
        return accumulator;
      },
      {
        runCount: 0,
        canonical: 0,
        provisional: 0,
        skipped: 0,
        national: 0,
        institutional: 0,
        rejected: 0,
        wallTimeMs: 0,
      }
    );

    return {
      ...totals,
      avgWallTimeMs: totals.runCount > 0 ? totals.wallTimeMs / totals.runCount : 0,
    };
  }, [crawlRuns, selectedBenchmark?.sourceSlug]);
  const chapterSearchComparison = useMemo(
    () =>
      computeChapterSearchComparison(crawlRuns, {
        sourceSlug: selectedBenchmark?.sourceSlug ?? null
      }),
    [crawlRuns, selectedBenchmark?.sourceSlug]
  );

  const gateReport = selectedDetail?.gateReport ?? null;
  const baselineBenchmark = selectedDetail?.baseline ?? null;
  const shadowDiffs = selectedDetail?.run?.shadowDiffs ?? selectedBenchmark?.shadowDiffs ?? [];
  const selectedAlerts = benchmarkAlerts;
  const queueEfficiency = useMemo(() => computeQueueEfficiency(summary), [summary]);
  const cycleBehavior = useMemo(() => {
    const samples = selectedBenchmark?.samples ?? [];
    if (samples.length === 0) {
      return {
        avgProcessed: 0,
        avgRequeued: 0,
        stallCycles: 0,
      };
    }
    const totals = samples.reduce(
      (accumulator, sample) => {
        accumulator.processed += sample.processed;
        accumulator.requeued += sample.requeued;
        if (sample.processed === 0 && sample.requeued > 0) {
          accumulator.stallCycles += 1;
        }
        return accumulator;
      },
      { processed: 0, requeued: 0, stallCycles: 0 }
    );
    return {
      avgProcessed: totals.processed / samples.length,
      avgRequeued: totals.requeued / samples.length,
      stallCycles: totals.stallCycles,
    };
  }, [selectedBenchmark?.samples]);
  const alertSummary = useMemo(() => ({
    info: selectedAlerts.filter((alert) => alert.severity === "info").length,
    warning: selectedAlerts.filter((alert) => alert.severity === "warning").length,
    critical: selectedAlerts.filter((alert) => alert.severity === "critical").length,
  }), [selectedAlerts]);

  const epochSeries = useMemo(() => [...adaptiveEpochs].reverse(), [adaptiveEpochs]);

  return (
    <div className="sectionStack">
      <section className="panel heroPanel">
        <h2>Benchmark Snapshot</h2>
        <p className="sectionDescription">Run repeatable queue benchmarks, compare throughput over time, and inspect per-cycle behavior for each run.</p>
        <div className="heroGrid">
          <div>
            <div className="metrics">
              <MetricCard label="Saved Benchmarks" value={counts.total} />
              <MetricCard label="Running / Queued" value={runningCount} />
              <MetricCard label="Active Campaigns" value={activeCampaignCount} />
              <MetricCard label="Latest Throughput" value={summary ? `${formatNumber(summary.jobsPerMinute, 1)} jobs/min` : "n/a"} />
              <MetricCard label="Best Throughput" value={`${formatNumber(bestThroughput, 1)} jobs/min`} />
              <MetricCard label="Best Queue Delta" value={formatNumber(bestQueueDelta)} />
              <MetricCard label="Open Critical Alerts" value={formatNumber(globalAlertSummary?.openCritical ?? 0)} />
              <MetricCard label="Open Warning Alerts" value={formatNumber(globalAlertSummary?.openWarning ?? 0)} />
              <MetricCard label="Resolved (24h)" value={formatNumber(globalAlertSummary?.resolvedLast24h ?? 0)} />
            </div>
          </div>
          <div className="heroAsideCard">
            <p className="eyebrow">Benchmark Use</p>
            <div className="heroChecklistItem">
              <strong>Throughput</strong>
              <span>Measure jobs/min and compare field-specific saturation.</span>
            </div>
            <div className="heroChecklistItem">
              <strong>Queue Delta</strong>
              <span>Negative deltas mean the benchmark is actually burning backlog down.</span>
            </div>
            <div className="heroChecklistItem">
              <strong>Regression Detection</strong>
              <span>Watch for requeue spikes and cycle-duration inflation before rollout.</span>
            </div>
            <div className="heroChecklistItem">
              <strong>Campaign Validation</strong>
              <span>Need broader proof? Use <a href="/campaigns">Campaigns</a> for multi-fraternity long-run tests.</span>
            </div>
            <div className="buttonRow">
              <button
                type="button"
                className="buttonSecondary"
                onClick={() => void handleScanAllAlerts()}
                disabled={isScanningAllAlerts}
              >
                {isScanningAllAlerts ? "Scanning all..." : "Scan All Benchmarks"}
              </button>
              <span className="cellHint">{globalAlertSummary ? `Updated ${formatTimestamp(globalAlertSummary.lastUpdatedAt)}` : "Alert summary loading..."}</span>
            </div>
          </div>
        </div>
      </section>

      <section className="panel benchmarkControls">
        <h2>Run New Benchmark</h2>
        <p className="sectionDescription">Configure a benchmark profile and launch it from here. New runs are saved and always appear first in history.</p>

        <form onSubmit={handleRunBenchmark}>
          <div className="benchmarkFormGrid">
            <div className="fieldStack">
              <label htmlFor="benchmark-name">Run Name</label>
              <input
                id="benchmark-name"
                value={form.name}
                onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
                placeholder="Optional label"
              />
            </div>

            <div className="fieldStack">
              <label htmlFor="benchmark-field">Target Field</label>
              <select
                id="benchmark-field"
                value={form.fieldName}
                onChange={(event) => setForm((current) => ({ ...current, fieldName: event.target.value as BenchmarkFieldName }))}
              >
                <option value="find_email">Email</option>
                <option value="find_instagram">Instagram</option>
                <option value="find_website">Website</option>
                <option value="all">All fields</option>
              </select>
            </div>

            <div className="fieldStack">
              <label htmlFor="benchmark-source">Source Slug</label>
              <input
                id="benchmark-source"
                value={form.sourceSlug}
                onChange={(event) => setForm((current) => ({ ...current, sourceSlug: event.target.value }))}
                placeholder="Optional source slug"
              />
            </div>

            <div className="fieldStack">
              <label htmlFor="benchmark-workers">Workers</label>
              <input
                id="benchmark-workers"
                type="number"
                min={1}
                max={16}
                value={form.workers}
                onChange={(event) => setForm((current) => ({ ...current, workers: Number(event.target.value) || 1 }))}
              />
            </div>

            <div className="fieldStack">
              <label htmlFor="benchmark-limit">Limit Per Cycle</label>
              <input
                id="benchmark-limit"
                type="number"
                min={1}
                max={500}
                value={form.limitPerCycle}
                onChange={(event) => setForm((current) => ({ ...current, limitPerCycle: Number(event.target.value) || 1 }))}
              />
            </div>

            <div className="fieldStack">
              <label htmlFor="benchmark-cycles">Cycles</label>
              <input
                id="benchmark-cycles"
                type="number"
                min={1}
                max={100}
                value={form.cycles}
                onChange={(event) => setForm((current) => ({ ...current, cycles: Number(event.target.value) || 1 }))}
              />
            </div>

            <div className="fieldStack">
              <label htmlFor="benchmark-pause">Pause (ms)</label>
              <input
                id="benchmark-pause"
                type="number"
                min={0}
                max={10000}
                value={form.pauseMs}
                onChange={(event) => setForm((current) => ({ ...current, pauseMs: Number(event.target.value) || 0 }))}
              />
            </div>
            <div className="fieldStack">
              <label htmlFor="benchmark-field-runtime">Field-Job Runtime</label>
              <select
                id="benchmark-field-runtime"
                value={form.fieldJobRuntimeMode}
                onChange={(event) =>
                  setForm((current) => ({
                    ...current,
                    fieldJobRuntimeMode: event.target.value as "langgraph_primary"
                  }))
                }
              >
                <option value="langgraph_primary">langgraph_primary</option>
              </select>
            </div>

            <div className="fieldStack">
              <label htmlFor="benchmark-field-durability">Graph Durability</label>
              <select
                id="benchmark-field-durability"
                value={form.fieldJobGraphDurability}
                onChange={(event) =>
                  setForm((current) => ({
                    ...current,
                    fieldJobGraphDurability: event.target.value as "exit" | "async" | "sync"
                  }))
                }
              >
                <option value="sync">sync</option>
                <option value="async">async</option>
                <option value="exit">exit</option>
              </select>
            </div>
          </div>

          <div className="buttonRow">
            <button type="submit" className="buttonPrimaryAuto" disabled={isSubmitting}>
              {isSubmitting ? "Starting..." : "Run Benchmark"}
            </button>
            <button type="button" className="buttonSecondary" disabled={isRefreshing} onClick={() => void refreshBenchmarkList()}>
              {isRefreshing ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </form>

        {errorMessage ? <p className="benchmarkError">{errorMessage}</p> : null}
      </section>

      <section className="benchmarkLayout">
        <article className="panel">
          <h2>All Benchmarks</h2>
          <p className="sectionDescription">Select any saved run to inspect full benchmark details and cycle-by-cycle performance.</p>

          {benchmarks.length === 0 ? (
            <p className="muted">No benchmarks yet. Run one to populate history.</p>
          ) : (
            <div className="benchmarkList">
              {benchmarks.map((item) => (
                <button
                  type="button"
                  key={item.id}
                  className={`benchmarkListItem${item.id === selectedBenchmark?.id ? " active" : ""}`}
                  onClick={() => setSelectedId(item.id)}
                >
                  <div className="benchmarkListItemHeader">
                    <strong>{item.name}</strong>
                    <StatusPill status={item.status} />
                  </div>
                  <div className="benchmarkListMeta">
                    <span>{FIELD_LABELS[item.fieldName]}</span>
                    <span>{formatTimestamp(item.createdAt)}</span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </article>

        <article className="panel">
          <h2>Benchmark Details</h2>
          {selectedBenchmark ? (
            <>
              <div className="benchmarkSelectedHeader">
                <div>
                  <h3>{selectedBenchmark.name}</h3>
                  <p className="muted">{FIELD_LABELS[selectedBenchmark.fieldName]} benchmark {selectedBenchmark.sourceSlug ? `for ${selectedBenchmark.sourceSlug}` : "across all sources"}</p>
                </div>
                <StatusPill status={selectedBenchmark.status} />
              </div>
              <div className="buttonRow">
                <a className="buttonSecondary" href={`/api/benchmarks/${selectedBenchmark.id}/export?format=json`} target="_blank" rel="noreferrer">
                  Export JSON Report
                </a>
                <a className="buttonSecondary" href={`/api/benchmarks/${selectedBenchmark.id}/export?format=md`} target="_blank" rel="noreferrer">
                  Export Markdown Report
                </a>
              </div>

              <div className="benchmarkMetaGrid">
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Started</p>
                  <p className="benchmarkMetaValue">{formatTimestamp(selectedBenchmark.startedAt)}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Finished</p>
                  <p className="benchmarkMetaValue">{formatTimestamp(selectedBenchmark.finishedAt)}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Workers</p>
                  <p className="benchmarkMetaValue">{selectedBenchmark.config.workers}</p>
                </div>
                <div className="benchmarkMetaCard">
                  <p className="benchmarkMetaLabel">Cycles x Limit</p>
                  <p className="benchmarkMetaValue">{selectedBenchmark.config.cycles} x {selectedBenchmark.config.limitPerCycle}</p>
                </div>
              </div>

              <div className="progressGrid">
                <ProgressMeter
                  label="Run Completion"
                  value={selectedBenchmark.samples.length}
                  total={selectedBenchmark.config.cycles}
                  hint={`${selectedBenchmark.samples.length} cycles captured`}
                />
                <ProgressMeter
                  label="Queue Burn-down"
                  value={Math.max(0, Math.abs(summary?.queueDepthDelta ?? 0))}
                  total={Math.max(1, Math.abs((summary?.queueDepthStart ?? 0) - (summary?.queueDepthEnd ?? 0)) || 1)}
                  hint={`delta ${formatNumber(summary?.queueDepthDelta)}`}
                />
              </div>

              <h3>Legacy vs Adaptive Runtime</h3>
              {runtimeComparison.totalRuns === 0 ? (
                <p className="muted">No crawl runs match this benchmark scope yet.</p>
              ) : (
                <>
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
                </>
              )}

              <h3>Chapter Search Slice</h3>
              {chapterSearchSummary.runCount === 0 ? (
                <p className="muted">No chapter-search telemetry found for this benchmark scope yet.</p>
              ) : (
                <>
                  <div className="comparisonGrid">
                    <div className="comparisonCard">
                      <div className="benchmarkListItemHeader">
                        <strong>Discovery Outcomes</strong>
                        <span className="cellHint">{chapterSearchSummary.runCount} runs</span>
                      </div>
                      <div className="comparisonMetrics">
                        <div>
                          <span className="comparisonLabel">Canonical</span>
                          <strong>{formatNumber(chapterSearchSummary.canonical)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Provisional</span>
                          <strong>{formatNumber(chapterSearchSummary.provisional)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Rejected</span>
                          <strong>{formatNumber(chapterSearchSummary.rejected)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Avg Wall Time</span>
                          <strong>{formatDuration(chapterSearchSummary.avgWallTimeMs)}</strong>
                        </div>
                      </div>
                    </div>
                    <div className="comparisonCard">
                      <div className="benchmarkListItemHeader">
                        <strong>Follow Breakdown</strong>
                        <span className="cellHint">chapter search only</span>
                      </div>
                      <div className="comparisonMetrics">
                        <div>
                          <span className="comparisonLabel">National</span>
                          <strong>{formatNumber(chapterSearchSummary.national)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Institutional</span>
                          <strong>{formatNumber(chapterSearchSummary.institutional)}</strong>
                        </div>
                        <div>
                          <span className="comparisonLabel">Skipped Sites</span>
                          <strong>{formatNumber(chapterSearchSummary.skipped)}</strong>
                        </div>
                      </div>
                    </div>
                  </div>
                </>
              )}
              {chapterSearchComparison.totalRuns > 0 ? (
                <>
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
              ) : null}

              <h3>Contact Resolution Slice</h3>
              <div className="comparisonGrid">
                <div className="comparisonCard">
                  <div className="benchmarkListItemHeader">
                    <strong>Queue Efficiency</strong>
                    <span className="cellHint">deferred contact work</span>
                  </div>
                  <div className="comparisonMetrics">
                    <div>
                      <span className="comparisonLabel">Processed</span>
                      <strong>{formatNumber(summary?.totalProcessed)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Requeued</span>
                      <strong>{formatNumber(summary?.totalRequeued)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Terminal</span>
                      <strong>{formatNumber(summary?.totalFailedTerminal)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Burn Down</span>
                      <strong>{formatNumber(queueEfficiency.burnDown)}</strong>
                    </div>
                  </div>
                </div>
                <div className="comparisonCard">
                  <div className="benchmarkListItemHeader">
                    <strong>Cycle Behavior</strong>
                    <span className="cellHint">queue churn guardrails</span>
                  </div>
                  <div className="comparisonMetrics">
                    <div>
                      <span className="comparisonLabel">Requeue Rate</span>
                      <strong>{formatPercent(queueEfficiency.requeueRate)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Avg Processed/Cycle</span>
                      <strong>{formatNumber(cycleBehavior.avgProcessed, 1)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Avg Requeued/Cycle</span>
                      <strong>{formatNumber(cycleBehavior.avgRequeued, 1)}</strong>
                    </div>
                    <div>
                      <span className="comparisonLabel">Stall Cycles</span>
                      <strong>{formatNumber(cycleBehavior.stallCycles)}</strong>
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
                      <td>{formatPercent(queueEfficiency.requeueRate)}</td>
                      <td>&lt;= 35% queue churn</td>
                      <td><strong>{queueEfficiency.requeueRate <= 0.35 ? "pass" : "fail"}</strong></td>
                    </tr>
                    <tr>
                      <td>Actionable Burn</td>
                      <td>{formatNumber(queueEfficiency.burnDown)}</td>
                      <td>&gt; 0 queued jobs burned</td>
                      <td><strong>{queueEfficiency.burnDown > 0 ? "pass" : "fail"}</strong></td>
                    </tr>
                    <tr>
                      <td>Cycle Stalls</td>
                      <td>{formatNumber(cycleBehavior.stallCycles)}</td>
                      <td>&lt;= 1 requeue-only cycle</td>
                      <td><strong>{cycleBehavior.stallCycles <= 1 ? "pass" : "fail"}</strong></td>
                    </tr>
                    <tr>
                      <td>Terminal Rate</td>
                      <td>{formatPercent(queueEfficiency.terminalRate)}</td>
                      <td>&lt;= 25% terminal exits</td>
                      <td><strong>{queueEfficiency.terminalRate <= 0.25 ? "pass" : "fail"}</strong></td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <h3>LangGraph Cutover Gates</h3>
              {gateReport ? (
                <>
                  <p className="muted">Baseline: {baselineBenchmark?.name ?? "n/a"}</p>
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
                        {gateReport.checks.map((check) => (
                          <tr key={check.label}>
                            <td>{check.label}</td>
                            <td>{check.value}</td>
                            <td>{check.target}</td>
                            <td><strong>{check.passed ? "pass" : "fail"}</strong></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              ) : (
                <p className="muted">Need at least one succeeded legacy benchmark with the same field/source scope to evaluate cutover gates.</p>
              )}

              <h3>Drift Alerts</h3>
              <div className="buttonRow">
                <select
                  value={alertSeverityFilter}
                  onChange={(event) => setAlertSeverityFilter(event.target.value as "info" | "warning" | "critical" | "all")}
                >
                  <option value="all">All severities</option>
                  <option value="critical">Critical</option>
                  <option value="warning">Warning</option>
                  <option value="info">Info</option>
                </select>
                <select
                  value={alertStatusFilter}
                  onChange={(event) => setAlertStatusFilter(event.target.value as "open" | "resolved" | "all")}
                >
                  <option value="open">Open only</option>
                  <option value="resolved">Resolved only</option>
                  <option value="all">Open + Resolved</option>
                </select>
                <button type="button" className="buttonSecondary" onClick={() => void handleScanAlerts()} disabled={isScanningAlerts}>
                  {isScanningAlerts ? "Scanning..." : "Run Drift Scan"}
                </button>
              </div>
              <p className="muted">
                Alerts in view: {selectedAlerts.length} (critical {alertSummary.critical}, warning {alertSummary.warning}, info {alertSummary.info})
              </p>
              {alertScanResult ? (
                <p className="muted">
                  Last scan: {formatTimestamp(alertScanResult.finishedAt)}. Considered {alertScanResult.consideredRuns} runs, opened {alertScanResult.alertsCreated}, resolved {alertScanResult.alertsResolved}.
                </p>
              ) : null}
              {selectedAlerts.length === 0 ? (
                <p className="muted">No alerts for the selected filters.</p>
              ) : (
                <div className="tableWrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Severity</th>
                        <th>Status</th>
                        <th>Type</th>
                        <th>Message</th>
                        <th>Created</th>
                        <th>Resolved</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedAlerts.map((alert) => {
                        const payload = alert.payload ?? {};
                        const resolvedByScanAt =
                          typeof payload.resolvedByScanAt === "string"
                            ? payload.resolvedByScanAt
                            : null;
                        return (
                          <tr key={alert.id}>
                            <td>{alert.severity}</td>
                            <td>{alert.status}</td>
                            <td>{alert.alertType}</td>
                            <td>{alert.message}</td>
                            <td>{formatTimestamp(alert.createdAt)}</td>
                            <td>{formatTimestamp(alert.resolvedAt ?? resolvedByScanAt)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
              <h3>Shadow Diff Artifacts</h3>
              {shadowDiffs.length === 0 ? (
                <p className="muted">No shadow diff artifacts captured for this benchmark yet.</p>
              ) : (
                <div className="tableWrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Cycle</th>
                        <th>Observed Jobs</th>
                        <th>Decision Mismatch</th>
                        <th>Status Mismatch</th>
                        <th>Mismatch Rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {shadowDiffs.map((row) => (
                        <tr key={`${row.id}-${row.cycle}`}>
                          <td>{row.cycle}</td>
                          <td>{row.observedJobs}</td>
                          <td>{row.decisionMismatchCount}</td>
                          <td>{row.statusMismatchCount}</td>
                          <td>{formatPercent(row.mismatchRate, 2)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              <h3>Field-Job Graph Timeline</h3>
              {graphRuns.length === 0 ? (
                <p className="muted">No field-job graph runs found for this benchmark scope.</p>
              ) : (
                <>
                  <div className="tableWrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Run ID</th>
                          <th>Runtime</th>
                          <th>Status</th>
                          <th>Worker</th>
                          <th>Events</th>
                          <th>Decisions</th>
                          <th>Created</th>
                        </tr>
                      </thead>
                      <tbody>
                        {graphRuns.slice(0, 8).map((row) => (
                          <tr key={row.id}>
                            <td>
                              <button type="button" className="buttonLink" onClick={() => {
                                void fetchGraphRunDetail(row.id)
                                  .then((detail) => setSelectedGraphRun(detail))
                                  .catch((error) => setErrorMessage(error instanceof Error ? error.message : String(error)));
                              }}>
                                {row.id}
                              </button>
                            </td>
                            <td>{row.runtimeMode}</td>
                            <td>{row.status}</td>
                            <td>{row.workerId}</td>
                            <td>{row.eventCount}</td>
                            <td>{row.decisionCount}</td>
                            <td>{formatTimestamp(row.createdAt)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {selectedGraphRun ? (
                    <div className="tableWrap">
                      <table>
                        <thead>
                          <tr>
                            <th>Node</th>
                            <th>Phase</th>
                            <th>Status</th>
                            <th>Latency (ms)</th>
                            <th>Job</th>
                            <th>At</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedGraphRun.events.slice(0, 20).map((event) => (
                            <tr key={event.id}>
                              <td>{event.nodeName}</td>
                              <td>{event.phase}</td>
                              <td>{event.status}</td>
                              <td>{event.latencyMs}</td>
                              <td>{event.jobId ?? "n/a"}</td>
                              <td>{formatTimestamp(event.createdAt)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}
                </>
              )}

              <h3>Adaptive Learning Curve</h3>
              {epochSeries.length === 0 ? (
                <p className="muted">No adaptive epoch metrics yet.</p>
              ) : (
                <div className="tableWrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Epoch</th>
                        <th>Balanced Score</th>
                        <th>Balanced Slope</th>
                        <th>Jobs/Min Delta</th>
                      </tr>
                    </thead>
                    <tbody>
                      {epochSeries.slice(-10).map((epoch) => (
                        <tr key={epoch.id}>
                          <td>{epoch.epoch}</td>
                          <td>{formatNumber(epoch.kpis?.balancedScore, 4)}</td>
                          <td>{formatNumber(epoch.slopes?.balancedScoreSlope, 4)}</td>
                          <td>{formatNumber(epoch.kpis?.jobsPerMinuteDelta, 4)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              <div className="tableWrap">
                <table>
                  <thead>
                    <tr>
                      <th>Elapsed</th>
                      <th>Processed</th>
                      <th>Requeued</th>
                      <th>Failed Terminal</th>
                      <th>Jobs / Min</th>
                      <th>Queue Delta</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>{formatDuration(summary?.elapsedMs)}</td>
                      <td>{formatNumber(summary?.totalProcessed)}</td>
                      <td>{formatNumber(summary?.totalRequeued)}</td>
                      <td>{formatNumber(summary?.totalFailedTerminal)}</td>
                      <td>{formatNumber(summary?.jobsPerMinute, 1)}</td>
                      <td>{formatNumber(summary?.queueDepthDelta)}</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <h3>Cycle Timeline</h3>
              {selectedBenchmark.samples.length === 0 ? (
                <p className="muted">No cycle samples captured yet.</p>
              ) : (
                <div className="tableWrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Cycle</th>
                        <th>Duration</th>
                        <th>Processed</th>
                        <th>Requeued</th>
                        <th>Failed Terminal</th>
                        <th>Queued</th>
                        <th>Running</th>
                        <th>Done</th>
                        <th>Failed</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedBenchmark.samples.map((sample) => (
                        <tr key={`${selectedBenchmark.id}-${sample.cycle}`}>
                          <td>{sample.cycle}</td>
                          <td>{formatDuration(sample.durationMs)}</td>
                          <td>{sample.processed}</td>
                          <td>{sample.requeued}</td>
                          <td>{sample.failedTerminal}</td>
                          <td>{sample.queued}</td>
                          <td>{sample.running}</td>
                          <td>{sample.done}</td>
                          <td>{sample.failed}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {selectedBenchmark.samples.length ? (
                <div className="cycleSparkline">
                  {selectedBenchmark.samples.map((sample) => {
                    const peak = Math.max(...selectedBenchmark.samples.map((item) => item.processed + item.failedTerminal + item.requeued), 1);
                    const total = sample.processed + sample.failedTerminal + sample.requeued;
                    return (
                      <div key={`${selectedBenchmark.id}-bar-${sample.cycle}`} className="cycleSparklineBarWrap">
                        <span className="cycleSparklineLabel">C{sample.cycle}</span>
                        <div className="cycleSparklineTrack">
                          <div className="cycleSparklineBar" style={{ height: `${Math.max(14, (total / peak) * 100)}%` }} />
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : null}

              {selectedBenchmark.lastError ? <p className="benchmarkError">{selectedBenchmark.lastError}</p> : null}
            </>
          ) : (
            <p className="muted">Pick a benchmark from the left to inspect details.</p>
          )}
        </article>
      </section>
    </div>
  );
}


































































