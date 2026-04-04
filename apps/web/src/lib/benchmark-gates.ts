import type {
  BenchmarkGateCheck,
  BenchmarkGateReport,
  BenchmarkRunListItem,
} from "@/lib/types";

function ratio(numerator: number, denominator: number): number {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) {
    return 0;
  }
  return numerator / denominator;
}

function computeP95DurationMs(samples: { durationMs: number }[]): number {
  if (!samples.length) {
    return 0;
  }
  const sorted = [...samples].map((sample) => sample.durationMs).sort((a, b) => a - b);
  const index = Math.max(0, Math.ceil(sorted.length * 0.95) - 1);
  return sorted[index] ?? 0;
}

function formatNumber(value: number, digits = 1): string {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatPercent(value: number, digits = 2): string {
  return `${formatNumber(value * 100, digits)}%`;
}

function hasComparableProtocol(current: BenchmarkRunListItem, baseline: BenchmarkRunListItem): boolean {
  return (
    current.config.workers === baseline.config.workers &&
    current.config.limitPerCycle === baseline.config.limitPerCycle &&
    current.config.cycles === baseline.config.cycles &&
    current.config.pauseMs === baseline.config.pauseMs &&
    Boolean(current.config.runAdaptiveCrawlBeforeCycles) === Boolean(baseline.config.runAdaptiveCrawlBeforeCycles)
  );
}

export function findLatestLegacyBaseline(
  runs: BenchmarkRunListItem[],
  current: BenchmarkRunListItem
): BenchmarkRunListItem | null {
  const source = current.sourceSlug ?? null;
  const candidates = runs
    .filter((item) => item.id !== current.id)
    .filter((item) => item.status === "succeeded")
    .filter((item) => (item.config.fieldJobRuntimeMode ?? "legacy") === "legacy")
    .filter((item) => item.fieldName === current.fieldName)
    .filter((item) => (item.sourceSlug ?? null) === source)
    .filter((item) => hasComparableProtocol(current, item))
    .sort((left, right) => new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime());

  return candidates[0] ?? null;
}

export function buildBenchmarkGateReport(
  current: BenchmarkRunListItem,
  baseline: BenchmarkRunListItem | null
): BenchmarkGateReport | null {
  if (!baseline || !current.summary || !baseline.summary) {
    return null;
  }

  const baselineSummary = baseline.summary;
  const currentSummary = current.summary;

  const queueStartDriftPct = baselineSummary.queueDepthStart > 0
    ? (Math.abs(currentSummary.queueDepthStart - baselineSummary.queueDepthStart) / baselineSummary.queueDepthStart) * 100
    : 0;

  const baselineCompleteCycles = baseline.samples.length >= baseline.config.cycles;
  const currentCompleteCycles = current.samples.length >= current.config.cycles;
  const comparisonQualityPassed = queueStartDriftPct <= 15 && baselineCompleteCycles && currentCompleteCycles;

  const throughputDeltaPct = ratio(
    currentSummary.jobsPerMinute - baselineSummary.jobsPerMinute,
    Math.max(0.0001, baselineSummary.jobsPerMinute)
  ) * 100;

  const baselineRetryWaste = ratio(
    baselineSummary.totalRequeued,
    baselineSummary.totalProcessed + baselineSummary.totalFailedTerminal + baselineSummary.totalRequeued
  );
  const currentRetryWaste = ratio(
    currentSummary.totalRequeued,
    currentSummary.totalProcessed + currentSummary.totalFailedTerminal + currentSummary.totalRequeued
  );
  const retryWasteReductionPct = baselineRetryWaste > 0 ? ((baselineRetryWaste - currentRetryWaste) / baselineRetryWaste) * 100 : 0;

  const baselineP95 = computeP95DurationMs(baseline.samples);
  const currentP95 = computeP95DurationMs(current.samples);
  const p95ImprovementPct = baselineP95 > 0 ? ((baselineP95 - currentP95) / baselineP95) * 100 : 0;

  const baselineTerminalRate = ratio(
    baselineSummary.totalFailedTerminal,
    baselineSummary.totalProcessed + baselineSummary.totalFailedTerminal
  );
  const currentTerminalRate = ratio(
    currentSummary.totalFailedTerminal,
    currentSummary.totalProcessed + currentSummary.totalFailedTerminal
  );

  const baselineBurn = Math.abs(Math.min(0, baselineSummary.queueDepthDelta));
  const currentBurn = Math.abs(Math.min(0, currentSummary.queueDepthDelta));
  const burnRetentionPct = baselineBurn > 0 ? (currentBurn / baselineBurn) * 100 : 100;

  const checks: BenchmarkGateCheck[] = [
    {
      label: "Comparison quality",
      value: `queue-start drift ${formatNumber(queueStartDriftPct, 1)}%; cycles ${current.samples.length}/${current.config.cycles}`,
      target: "drift <= 15% and full cycle completion",
      passed: comparisonQualityPassed,
    },
    {
      label: "Throughput uplift",
      value: `${formatNumber(throughputDeltaPct, 1)}%`,
      target: ">= 30%",
      passed: throughputDeltaPct >= 30,
    },
    {
      label: "Retry waste reduction",
      value: `${formatNumber(retryWasteReductionPct, 1)}%`,
      target: ">= 60%",
      passed: retryWasteReductionPct >= 60,
    },
    {
      label: "p95 latency improvement",
      value: `${formatNumber(p95ImprovementPct, 1)}%`,
      target: ">= 25%",
      passed: p95ImprovementPct >= 25,
    },
    {
      label: "Queue burn retention",
      value: `${formatNumber(burnRetentionPct, 1)}%`,
      target: ">= 90% of legacy burn",
      passed: burnRetentionPct >= 90,
    },
    {
      label: "Terminal rate safety",
      value: `${formatPercent(currentTerminalRate, 2)} (legacy ${formatPercent(baselineTerminalRate, 2)})`,
      target: "non-inferior",
      passed: currentTerminalRate <= baselineTerminalRate + 0.01,
    },
  ];

  return {
    benchmarkId: current.id,
    benchmarkName: current.name,
    baselineBenchmarkId: baseline.id,
    baselineBenchmarkName: baseline.name,
    checks,
  };
}

export function renderBenchmarkGateMarkdown(params: {
  run: BenchmarkRunListItem;
  baseline: BenchmarkRunListItem | null;
  gateReport: BenchmarkGateReport | null;
}): string {
  const { run, baseline, gateReport } = params;
  const lines: string[] = [];
  lines.push(`# Benchmark Promotion Gate Report: ${run.name}`);
  lines.push("");
  lines.push(`- Benchmark ID: ${run.id}`);
  lines.push(`- Status: ${run.status}`);
  lines.push(`- Field: ${run.fieldName}`);
  lines.push(`- Source: ${run.sourceSlug ?? "all"}`);
  lines.push(`- Runtime: ${run.config.fieldJobRuntimeMode ?? "legacy"}`);
  lines.push(`- Created: ${run.createdAt}`);
  lines.push("");

  if (!gateReport) {
    lines.push("No legacy baseline is available for this field/source scope.");
    lines.push("");
  } else {
    lines.push(`Baseline: ${baseline?.name ?? "n/a"} (${baseline?.id ?? "n/a"})`);
    lines.push("");
    lines.push("| Gate | Value | Target | Status |");
    lines.push("| --- | --- | --- | --- |");
    for (const check of gateReport.checks) {
      lines.push(`| ${check.label} | ${check.value} | ${check.target} | ${check.passed ? "pass" : "fail"} |`);
    }
    lines.push("");
  }

  if (run.shadowDiffs?.length) {
    lines.push("## Shadow Diff Artifacts");
    lines.push("| Cycle | Observed Jobs | Decision Mismatch | Status Mismatch | Mismatch Rate |");
    lines.push("| ---: | ---: | ---: | ---: | ---: |");
    for (const row of run.shadowDiffs) {
      lines.push(`| ${row.cycle} | ${row.observedJobs} | ${row.decisionMismatchCount} | ${row.statusMismatchCount} | ${formatNumber(row.mismatchRate * 100, 2)}% |`);
    }
    lines.push("");
  }

  lines.push("## Summary JSON");
  lines.push("```json");
  lines.push(JSON.stringify({ run, baseline, gateReport }, null, 2));
  lines.push("```");
  lines.push("");

  return lines.join("\n");
}
