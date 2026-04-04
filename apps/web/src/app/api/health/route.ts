import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleBenchmarkDriftAlertScan } from "@/lib/benchmark-alerts";
import { activeCampaignRunCount, scheduleDueCampaignRuns } from "@/lib/campaign-runner";
import { reconcileStaleCampaignRuns } from "@/lib/repositories/campaign-run-repository";

export async function GET() {
  try {
    const reconciledCampaigns = await reconcileStaleCampaignRuns();
    const scheduledCampaigns = await scheduleDueCampaignRuns();
    const benchmarkDriftScan = await scheduleBenchmarkDriftAlertScan();

    return apiSuccess({
      ok: true,
      runtime: {
        activeCampaignRuns: activeCampaignRunCount(),
        reconciledCampaigns,
        scheduledCampaigns,
        benchmarkDriftScan,
      },
      checkedAt: new Date().toISOString()
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
