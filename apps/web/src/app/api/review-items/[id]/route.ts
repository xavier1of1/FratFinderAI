import { reviewStatusSchema } from "@fratfinder/contracts";
import { NextRequest } from "next/server";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { updateReviewItemStatusWithAudit } from "@/lib/repositories/review-item-repository";

const bodySchema = z.object({
  status: reviewStatusSchema,
  triageNotes: z.string().optional(),
  resolvedBy: z.string().optional(),
  actor: z.string().min(1).optional(),
  notes: z.string().optional()
});

export async function PATCH(request: NextRequest, context: { params: { id: string } }) {
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
