import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { dispatchCrmCampaign } from "@/lib/repositories/crm-repository";
import { withOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

const payloadSchema = z.object({
  mode: z.enum(["draft", "send"]).default("draft")
});

async function dispatchCrmCampaignHandler(request: Request, context: { params: { id: string } }) {
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

export const POST = withOperatorAccess(
  ["admin"],
  "crm_campaign_dispatch",
  (_request, context: { params: { id: string } }) => ({ targetType: "crm_campaign", targetId: context.params.id }),
  dispatchCrmCampaignHandler
);
