import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { countActiveRuntimeWorkersByLane } from "@/lib/repositories/runtime-worker-repository";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const activeWorkers = await countActiveRuntimeWorkersByLane(["campaign", "benchmark", "evaluation"]);
    return apiSuccess({
      ok: true,
      runtime: {
        activeCampaignRuns: Number(activeWorkers.campaign ?? 0),
        activeBenchmarkRuns: Number(activeWorkers.benchmark ?? 0),
        activeEvaluationWorkers: Number(activeWorkers.evaluation ?? 0),
        mutatingReadPathsDisabled: true,
      },
      checkedAt: new Date().toISOString()
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
