import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { cancelCampaignRun, isCampaignRunActive } from "@/lib/campaign-runner";
import { getCampaignRun } from "@/lib/repositories/campaign-run-repository";

export async function POST(_request: Request, context: { params: { id: string } }) {
  try {
    await cancelCampaignRun(context.params.id);
    const campaign = await getCampaignRun(context.params.id);
    return apiSuccess({
      campaign,
      active: isCampaignRunActive(context.params.id)
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
