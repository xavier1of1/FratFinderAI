import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { isCampaignRunActive, scheduleCampaignRun } from "@/lib/campaign-runner";
import { getCampaignRun, reconcileStaleCampaignRuns } from "@/lib/repositories/campaign-run-repository";

export async function GET(_request: Request, context: { params: { id: string } }) {
  try {
    await reconcileStaleCampaignRuns();
    const campaign = await getCampaignRun(context.params.id);
    if (!campaign) {
      throw new Error(`Campaign run ${context.params.id} not found`);
    }

    if (campaign.status === "queued") {
      await scheduleCampaignRun(campaign.id);
    } else if (campaign.status === "running" && !isCampaignRunActive(campaign.id)) {
      await scheduleCampaignRun(campaign.id);
    }

    return apiSuccess({
      ...campaign,
      runtimeActive: isCampaignRunActive(campaign.id)
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
