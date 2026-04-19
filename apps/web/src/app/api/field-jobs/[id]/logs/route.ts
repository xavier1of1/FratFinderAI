import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getFieldJobLogFeed } from "@/lib/repositories/field-job-repository";

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: { params: { id: string } }) {
  try {
    const jobId = context.params.id?.trim();
    if (!jobId) {
      return apiError({ status: 400, code: "invalid_id", message: "Field job id is required" });
    }

    const url = new URL(request.url);
    const limit = Number(url.searchParams.get("limit") ?? "80");
    const data = await getFieldJobLogFeed(jobId, Number.isFinite(limit) ? limit : 80);
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
