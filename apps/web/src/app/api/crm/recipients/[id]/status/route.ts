import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { updateCrmRecipientStatus } from "@/lib/repositories/crm-repository";

export const dynamic = "force-dynamic";

const payloadSchema = z.object({
  status: z.enum(["queued", "drafted", "sent", "failed"]),
  lastError: z.string().trim().max(1000).nullable().optional().or(z.literal(""))
});

export async function POST(request: Request, context: { params: { id: string } }) {
  try {
    const payload = payloadSchema.parse(await request.json());
    const recipient = await updateCrmRecipientStatus({
      recipientId: context.params.id,
      status: payload.status,
      lastError: payload.lastError || null
    });
    if (!recipient) {
      throw new Error("CRM recipient not found");
    }
    return apiSuccess(recipient);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
