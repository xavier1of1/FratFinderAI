import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCrmCampaignCounts } from "@/lib/repositories/crm-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

async function getCrmCampaignSummaryHandler() {
  try {
    const counts = await getCrmCampaignCounts();
    return apiSuccess(counts);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess("dashboard_read", { targetType: "crm_campaign_summary" }, getCrmCampaignSummaryHandler);
