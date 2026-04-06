import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { cancelCampaignRun } from "@/lib/campaign-runner";
import { cancelEvaluationJob, getEvaluationJobByRun } from "@/lib/repositories/evaluation-job-repository";
import { getCampaignRun } from "@/lib/repositories/campaign-run-repository";

export async function POST(_request: Request, context: { params: { id: string } }) {
  try {
    const evaluationJob = await getEvaluationJobByRun({ campaignRunId: context.params.id });
    if (evaluationJob) {
      await cancelEvaluationJob(evaluationJob.id);
    }
    await cancelCampaignRun(context.params.id);
    const campaign = await getCampaignRun(context.params.id);
    const runtimeActive =
      Boolean(campaign?.runtimeWorkerId) &&
      (!campaign?.runtimeLeaseExpiresAt || new Date(campaign.runtimeLeaseExpiresAt).getTime() >= Date.now());
    return apiSuccess({
      campaign,
      active: runtimeActive
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
