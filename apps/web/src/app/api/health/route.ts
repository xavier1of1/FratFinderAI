import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { activeCampaignRunCount, scheduleDueCampaignRuns } from "@/lib/campaign-runner";
import { reconcileStaleCampaignRuns } from "@/lib/repositories/campaign-run-repository";

export async function GET() {
  try {
    const reconciledCampaigns = await reconcileStaleCampaignRuns();
    const scheduledCampaigns = await scheduleDueCampaignRuns();

    return apiSuccess({
      ok: true,
      runtime: {
        activeCampaignRuns: activeCampaignRunCount(),
        reconciledCampaigns,
        scheduledCampaigns
      },
      checkedAt: new Date().toISOString()
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
