import { buildBenchmarkGateReport, findLatestLegacyBaseline } from "@/lib/benchmark-gates";
import {
  getBenchmarkRun,
  listBenchmarkRuns,
  resolveBenchmarkAlertsByFingerprintPrefix,
  upsertOpenBenchmarkAlert,
} from "@/lib/repositories/benchmark-repository";

function slugify(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function envScanIntervalMs(): number {
  const raw = Number(process.env.BENCHMARK_ALERT_SCAN_INTERVAL_MS ?? 60_000);
  if (!Number.isFinite(raw)) {
    return 60_000;
  }
  return Math.max(5_000, Math.min(raw, 15 * 60_000));
}

function isLangGraphMode(value: string | undefined): boolean {
  return value === "langgraph_shadow" || value === "langgraph_primary";
}

export interface BenchmarkDriftScanResult {
  startedAt: string;
  finishedAt: string;
  consideredRuns: number;
  alertsCreated: number;
  alertsResolved: number;
}

let lastScanAtMs = 0;
let inFlightScan: Promise<BenchmarkDriftScanResult> | null = null;

export async function runBenchmarkDriftAlertScan(limit = 60): Promise<BenchmarkDriftScanResult> {
  const runs = await listBenchmarkRuns(Math.max(10, Math.min(500, limit)));
  const startedAt = new Date().toISOString();
  let alertsCreated = 0;
  let alertsResolved = 0;
  let consideredRuns = 0;

  for (const run of runs) {
    if (run.status !== "succeeded") {
      continue;
    }

    if (!isLangGraphMode(run.config.fieldJobRuntimeMode)) {
      continue;
    }

    const detailed = await getBenchmarkRun(run.id);
    if (!detailed) {
      continue;
    }

    consideredRuns += 1;
    const fingerprintPrefix = `benchmark:${run.id}:`;
    alertsResolved += await resolveBenchmarkAlertsByFingerprintPrefix({
      prefix: fingerprintPrefix,
      resolvedReason: "drift_recheck",
      metadata: {
        benchmarkId: run.id,
        runtimeMode: detailed.config.fieldJobRuntimeMode ?? "legacy",
      },
    });

    const baseline = findLatestLegacyBaseline(runs, detailed);
    const gateReport = buildBenchmarkGateReport(detailed, baseline);

    if (!gateReport) {
      await upsertOpenBenchmarkAlert({
        benchmarkRunId: detailed.id,
        alertType: "missing_legacy_baseline",
        severity: "info",
        message: "No comparable succeeded legacy benchmark exists for this field/source scope yet.",
        fingerprint: `${fingerprintPrefix}baseline-missing`,
        payload: {
          fieldName: detailed.fieldName,
          sourceSlug: detailed.sourceSlug,
          runtimeMode: detailed.config.fieldJobRuntimeMode ?? "legacy",
        },
      });
      alertsCreated += 1;
    } else {
      for (const check of gateReport.checks) {
        if (check.passed) {
          continue;
        }

        const checkSlug = slugify(check.label);
        const severity = check.label === "Terminal rate safety" ? "critical" : "warning";
        await upsertOpenBenchmarkAlert({
          benchmarkRunId: detailed.id,
          alertType: "cutover_gate_failed",
          severity,
          message: `Cutover gate failed: ${check.label} (value ${check.value}, target ${check.target}).`,
          fingerprint: `${fingerprintPrefix}gate-${checkSlug}`,
          payload: {
            check,
            baselineBenchmarkId: baseline?.id ?? null,
            baselineBenchmarkName: baseline?.name ?? null,
            benchmarkId: detailed.id,
            benchmarkName: detailed.name,
          },
        });
        alertsCreated += 1;
      }
    }

    const shadowDiffs = detailed.shadowDiffs ?? [];
    if ((detailed.config.fieldJobRuntimeMode ?? "legacy") === "langgraph_shadow" && shadowDiffs.length === 0) {
      await upsertOpenBenchmarkAlert({
        benchmarkRunId: detailed.id,
        alertType: "shadow_diff_missing",
        severity: "warning",
        message: "LangGraph shadow benchmark has no shadow diff artifacts; comparison coverage is incomplete.",
        fingerprint: `${fingerprintPrefix}shadow-diff-missing`,
        payload: {
          benchmarkId: detailed.id,
          runtimeMode: detailed.config.fieldJobRuntimeMode,
        },
      });
      alertsCreated += 1;
    }

    if (shadowDiffs.length > 0) {
      const last = shadowDiffs[shadowDiffs.length - 1]!;
      const avgRate = shadowDiffs.reduce((sum, row) => sum + row.mismatchRate, 0) / shadowDiffs.length;

      if (last.mismatchRate >= 0.25 || avgRate >= 0.2) {
        await upsertOpenBenchmarkAlert({
          benchmarkRunId: detailed.id,
          alertType: "high_shadow_mismatch",
          severity: "critical",
          message: `Shadow mismatch is critically high (latest ${(last.mismatchRate * 100).toFixed(2)}%, average ${(avgRate * 100).toFixed(2)}%).`,
          fingerprint: `${fingerprintPrefix}shadow-mismatch-critical`,
          payload: {
            latestMismatchRate: last.mismatchRate,
            averageMismatchRate: avgRate,
            observedJobs: last.observedJobs,
            latestCycle: last.cycle,
          },
        });
        alertsCreated += 1;
      } else if (last.mismatchRate >= 0.1 || avgRate >= 0.08) {
        await upsertOpenBenchmarkAlert({
          benchmarkRunId: detailed.id,
          alertType: "high_shadow_mismatch",
          severity: "warning",
          message: `Shadow mismatch is elevated (latest ${(last.mismatchRate * 100).toFixed(2)}%, average ${(avgRate * 100).toFixed(2)}%).`,
          fingerprint: `${fingerprintPrefix}shadow-mismatch-warning`,
          payload: {
            latestMismatchRate: last.mismatchRate,
            averageMismatchRate: avgRate,
            observedJobs: last.observedJobs,
            latestCycle: last.cycle,
          },
        });
        alertsCreated += 1;
      }
    }
  }

  return {
    startedAt,
    finishedAt: new Date().toISOString(),
    consideredRuns,
    alertsCreated,
    alertsResolved,
  };
}

export async function scheduleBenchmarkDriftAlertScan(options?: {
  force?: boolean;
  limit?: number;
}): Promise<BenchmarkDriftScanResult | null> {
  const force = options?.force === true;
  const now = Date.now();
  const intervalMs = envScanIntervalMs();

  if (!force && now - lastScanAtMs < intervalMs) {
    return null;
  }

  if (inFlightScan) {
    return inFlightScan;
  }

  inFlightScan = runBenchmarkDriftAlertScan(options?.limit ?? 60)
    .then((result) => {
      lastScanAtMs = Date.now();
      return result;
    })
    .finally(() => {
      inFlightScan = null;
    });

  return inFlightScan;
}

