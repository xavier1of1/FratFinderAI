import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getBenchmarkRun } from "@/lib/repositories/benchmark-repository";

export async function GET(_: Request, context: { params: { id: string } }) {
  try {
    const run = await getBenchmarkRun(context.params.id);
    if (!run) {
      return apiError({
        status: 404,
        code: "not_found",
        message: `Benchmark ${context.params.id} not found`
      });
    }

    return apiSuccess(run);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}