import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleBenchmarkDriftAlertScan } from "@/lib/benchmark-alerts";
import { listBenchmarkAlerts } from "@/lib/repositories/benchmark-repository";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "100");
    const statusParam = searchParams.get("status") ?? "open";
    const status = statusParam === "all" || statusParam === "resolved" || statusParam === "open" ? statusParam : "open";
    const benchmarkRunId = searchParams.get("benchmarkRunId");
    const severityParam = searchParams.get("severity") ?? "all";
    const severity = severityParam === "info" || severityParam === "warning" || severityParam === "critical" || severityParam === "all" ? severityParam : "all";
    const scan = searchParams.get("scan") === "1";

    const scanResult = scan ? await scheduleBenchmarkDriftAlertScan({ force: true, limit: 120 }) : null;
    const alerts = await listBenchmarkAlerts({
      limit: Number.isFinite(limit) ? Math.max(1, Math.min(500, limit)) : 100,
      status,
      benchmarkRunId: benchmarkRunId?.trim() ? benchmarkRunId.trim() : null,
      severity,
    });

    return apiSuccess({
      alerts,
      scanResult,
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
