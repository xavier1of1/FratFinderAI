import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCampaignRun } from "@/lib/repositories/campaign-run-repository";

export const dynamic = "force-dynamic";

export async function GET(_request: Request, context: { params: { id: string } }) {
  try {
    const campaign = await getCampaignRun(context.params.id);
    if (!campaign) {
      throw new Error(`Campaign run ${context.params.id} not found`);
    }

    const runtimeActive =
      Boolean(campaign.runtimeWorkerId) &&
      (!campaign.runtimeLeaseExpiresAt || new Date(campaign.runtimeLeaseExpiresAt).getTime() >= Date.now());
    return apiSuccess({
      ...campaign,
      runtimeActive
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
