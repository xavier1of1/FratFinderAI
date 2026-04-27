import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCrmCampaign } from "@/lib/repositories/crm-repository";

export const dynamic = "force-dynamic";

export async function GET(_request: Request, context: { params: { id: string } }) {
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
