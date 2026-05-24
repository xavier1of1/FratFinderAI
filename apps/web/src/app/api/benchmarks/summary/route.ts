import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getBenchmarkRunCounts } from "@/lib/repositories/benchmark-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

async function getBenchmarkSummaryHandler() {
  try {
    const data = await getBenchmarkRunCounts();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess("dashboard_read", { targetType: "benchmark_summary" }, getBenchmarkSummaryHandler);
