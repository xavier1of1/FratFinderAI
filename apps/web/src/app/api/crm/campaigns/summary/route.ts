import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCrmCampaignCounts } from "@/lib/repositories/crm-repository";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const counts = await getCrmCampaignCounts();
    return apiSuccess(counts);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
