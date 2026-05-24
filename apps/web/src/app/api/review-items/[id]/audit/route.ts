import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listReviewItemAuditLogs } from "@/lib/repositories/review-item-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

async function getReviewItemAuditHandler(_request: Request, context: { params: { id: string } }) {
  try {
    const { id } = context.params;
    const data = await listReviewItemAuditLogs(id);
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess(
  "dashboard_read",
  (_request, context: { params: { id: string } }) => ({ targetType: "review_item", targetId: context.params.id }),
  getReviewItemAuditHandler
);
