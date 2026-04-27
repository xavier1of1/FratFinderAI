import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { dispatchCrmCampaign } from "@/lib/repositories/crm-repository";

export const dynamic = "force-dynamic";

const payloadSchema = z.object({
  mode: z.enum(["draft", "send"]).default("draft")
});

export async function POST(request: Request, context: { params: { id: string } }) {
  try {
    const payload = payloadSchema.parse(await request.json());
    const result = await dispatchCrmCampaign({
      id: context.params.id,
      mode: payload.mode
    });
    return apiSuccess(result);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
