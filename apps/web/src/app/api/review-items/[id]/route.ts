import { reviewStatusSchema } from "@fratfinder/contracts";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { updateReviewItemStatusWithAudit } from "@/lib/repositories/review-item-repository";
import { withOperatorAccess } from "@/lib/security/operator-access";

const bodySchema = z.object({
  status: reviewStatusSchema,
  triageNotes: z.string().optional(),
  resolvedBy: z.string().optional(),
  actor: z.string().min(1).optional(),
  notes: z.string().optional()
});

async function patchReviewItemHandler(request: Request, context: { params: { id: string } }) {
  try {
    const { id } = context.params;
    const json = await request.json();
    const payload = bodySchema.parse(json);

    const result = await updateReviewItemStatusWithAudit({
      id,
      status: payload.status,
      triageNotes: payload.triageNotes,
      resolvedBy: payload.resolvedBy,
      actor: payload.actor ?? process.env.WEB_DEFAULT_ACTOR ?? "local-operator",
      notes: payload.notes
    });

    return apiSuccess(result);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const PATCH = withOperatorAccess(
  ["operator", "admin"],
  "review_item_update",
  (_request, context: { params: { id: string } }) => ({ targetType: "review_item", targetId: context.params.id }),
  patchReviewItemHandler
);
