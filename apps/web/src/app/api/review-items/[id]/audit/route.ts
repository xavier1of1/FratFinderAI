import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listReviewItemAuditLogs } from "@/lib/repositories/review-item-repository";

export async function GET(_request: Request, context: { params: { id: string } }) {
  try {
    const { id } = context.params;
    const data = await listReviewItemAuditLogs(id);
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}