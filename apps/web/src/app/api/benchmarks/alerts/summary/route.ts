import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getBenchmarkAlertSummary } from "@/lib/repositories/benchmark-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

async function getBenchmarkAlertSummaryHandler() {
  try {
    const summary = await getBenchmarkAlertSummary();
    return apiSuccess(summary);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess("dashboard_read", { targetType: "benchmark_alert_summary" }, getBenchmarkAlertSummaryHandler);
