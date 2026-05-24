import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCampaignRun } from "@/lib/repositories/campaign-run-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

async function getCampaignRunHandler(_request: Request, context: { params: { id: string } }) {
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

export const GET = withReadOnlyOperatorAccess(
  "dashboard_read",
  (_request, context: { params: { id: string } }) => ({ targetType: "campaign_run", targetId: context.params.id }),
  getCampaignRunHandler
);
