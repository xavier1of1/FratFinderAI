import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleCampaignRun } from "@/lib/campaign-runner";
import { getCampaignRun, reconcileStaleCampaignRuns, updateCampaignRun } from "@/lib/repositories/campaign-run-repository";

export async function POST(_request: Request, context: { params: { id: string } }) {
  try {
    await reconcileStaleCampaignRuns();
    const campaign = await getCampaignRun(context.params.id);
    if (!campaign) {
      throw new Error(`Campaign run ${context.params.id} not found`);
    }
    if (campaign.status === "canceled") {
      return apiError({
        status: 409,
        code: "campaign_not_resumable",
        message: "Canceled campaigns are not resumable yet. Duplicate the campaign instead."
      });
    }

    await updateCampaignRun({
      id: campaign.id,
      status: "queued",
      scheduledFor: new Date().toISOString(),
      clearFinishedAt: true,
      lastError: null
    });

    await scheduleCampaignRun(campaign.id);
    const refreshed = await getCampaignRun(campaign.id);
    return apiSuccess(refreshed ?? campaign, { status: 202 });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
