import { buildBenchmarkGateReport, findLatestLegacyBaseline } from "@/lib/benchmark-gates";
import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { failStaleBenchmarkRuns, getBenchmarkRun, listBenchmarkRuns } from "@/lib/repositories/benchmark-repository";

export async function GET(request: Request, context: { params: { id: string } }) {
  try {
    await failStaleBenchmarkRuns();
    const run = await getBenchmarkRun(context.params.id);
    if (!run) {
      return apiError({
        status: 404,
        code: "not_found",
        message: `Benchmark ${context.params.id} not found`
      });
    }

    const includeComparisons = new URL(request.url).searchParams.get("includeComparisons") === "1";
    if (!includeComparisons) {
      return apiSuccess(run);
    }

    const allRuns = await listBenchmarkRuns(500);
    const baseline = findLatestLegacyBaseline(allRuns, run);
    const gateReport = buildBenchmarkGateReport(run, baseline);

    return apiSuccess({ run, baseline, gateReport });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
