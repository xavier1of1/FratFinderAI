import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCampaignRunCounts } from "@/lib/repositories/campaign-run-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

async function getCampaignSummaryHandler() {
  try {
    const data = await getCampaignRunCounts();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess("dashboard_read", { targetType: "campaign_summary" }, getCampaignSummaryHandler);
