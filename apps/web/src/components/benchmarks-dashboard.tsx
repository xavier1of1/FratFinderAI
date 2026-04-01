"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

import { MetricCard } from "@/components/metric-card";
import { ProgressMeter } from "@/components/progress-meter";
import { StatusPill } from "@/components/status-pill";
import { computeRuntimeComparison } from "@/lib/runtime-comparison";
import type { AdaptiveEpochMetric, BenchmarkFieldName, BenchmarkRunConfig, BenchmarkRunListItem, CrawlRunListItem } from "@/lib/types";

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

type BenchmarkFormState = {
  name: string;
  fieldName: BenchmarkFieldName;
  sourceSlug: string;
  workers: number;
  limitPerCycle: number;
  cycles: number;
  pauseMs: number;
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

export function BenchmarksDashboard({
  initialBenchmarks,
  initialRuns,
  activeCampaignCount = 0
}: {
  initialBenchmarks: BenchmarkRunListItem[];
  initialRuns: CrawlRunListItem[];
  activeCampaignCount?: number;
}) {
  const [benchmarks, setBenchmarks] = useState<BenchmarkRunListItem[]>(sortBenchmarks(initialBenchmarks));
  const [crawlRuns, setCrawlRuns] = useState<CrawlRunListItem[]>(initialRuns);
  const [adaptiveEpochs, setAdaptiveEpochs] = useState<AdaptiveEpochMetric[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(initialBenchmarks[0]?.id ?? null);
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
    pauseMs: 500
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

  const runningCount = useMemo(
    () => benchmarks.filter((item) => item.status === "running" || item.status === "queued").length,
    [benchmarks]
  );

  async function refreshBenchmarkList(options?: { selectNewest?: boolean }) {
    setIsRefreshing(true);
    try {
      const [benchmarkData, runData, epochData] = await Promise.all([fetchBenchmarks(), fetchCrawlRuns(), fetchAdaptiveEpochs()]);
      setBenchmarks(benchmarkData);
      setCrawlRuns(runData);
      setAdaptiveEpochs(epochData);

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
    if (adaptiveEpochs.length > 0) {
      return;
    }
    void fetchAdaptiveEpochs()
      .then((rows) => setAdaptiveEpochs(rows))
      .catch((error) => setErrorMessage(error instanceof Error ? error.message : String(error)));
  }, [adaptiveEpochs.length]);

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
        pauseMs: form.pauseMs
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
  const epochSeries = useMemo(() => [...adaptiveEpochs].reverse(), [adaptiveEpochs]);

  return (
    <div className="sectionStack">
      <section className="panel heroPanel">
        <h2>Benchmark Snapshot</h2>
        <p className="sectionDescription">Run repeatable queue benchmarks, compare throughput over time, and inspect per-cycle behavior for each run.</p>
        <div className="heroGrid">
          <div>
            <div className="metrics">
              <MetricCard label="Saved Benchmarks" value={benchmarks.length} />
              <MetricCard label="Running / Queued" value={runningCount} />
              <MetricCard label="Active Campaigns" value={activeCampaignCount} />
              <MetricCard label="Latest Throughput" value={summary ? `${formatNumber(summary.jobsPerMinute, 1)} jobs/min` : "n/a"} />
              <MetricCard label="Best Throughput" value={`${formatNumber(bestThroughput, 1)} jobs/min`} />
              <MetricCard label="Best Queue Delta" value={formatNumber(bestQueueDelta)} />
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



















