import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getFieldJobGraphRunDetail } from "@/lib/repositories/field-job-graph-repository";

export async function GET(request: Request, context: { params: { id: string } }) {
  try {
    const runId = Number(context.params.id);
    if (!Number.isFinite(runId) || runId <= 0) {
      return apiError({ status: 400, code: "invalid_id", message: "Run id must be a positive number" });
    }

    const url = new URL(request.url);
    const eventLimit = Number(url.searchParams.get("eventLimit") ?? "200");
    const decisionLimit = Number(url.searchParams.get("decisionLimit") ?? "200");

    const detail = await getFieldJobGraphRunDetail(runId, {
      eventLimit: Number.isFinite(eventLimit) ? eventLimit : 200,
      decisionLimit: Number.isFinite(decisionLimit) ? decisionLimit : 200,
    });

    if (!detail) {
      return apiError({ status: 404, code: "not_found", message: `Field-job graph run ${runId} not found` });
    }

    return apiSuccess(detail);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
