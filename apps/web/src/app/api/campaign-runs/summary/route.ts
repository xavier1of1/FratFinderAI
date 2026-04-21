import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getCampaignRunCounts } from "@/lib/repositories/campaign-run-repository";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const data = await getCampaignRunCounts();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
