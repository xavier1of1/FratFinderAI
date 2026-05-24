import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCrmCampaign } from "@/lib/repositories/crm-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

async function getCrmCampaignHandler(_request: Request, context: { params: { id: string } }) {
  try {
    const campaign = await getCrmCampaign(context.params.id);
    if (!campaign) {
      throw new Error("CRM campaign not found");
    }
    return apiSuccess(campaign);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess(
  "dashboard_read",
  (_request, context: { params: { id: string } }) => ({ targetType: "crm_campaign", targetId: context.params.id }),
  getCrmCampaignHandler
);
