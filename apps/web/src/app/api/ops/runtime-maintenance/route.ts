import { z } from "zod";

import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleBenchmarkDriftAlertScan } from "@/lib/benchmark-alerts";
import { scheduleDueCampaignRuns } from "@/lib/campaign-runner";
import { failStaleBenchmarkRuns } from "@/lib/repositories/benchmark-repository";
import { reconcileStaleCampaignRuns } from "@/lib/repositories/campaign-run-repository";
import { failStaleCrawlRuns } from "@/lib/repositories/crawl-run-repository";

const maintenancePayloadSchema = z.object({
  actions: z
    .array(
      z.enum([
        "reconcile_stale_campaign_runs",
        "schedule_due_campaign_runs",
        "fail_stale_benchmark_runs",
        "fail_stale_crawl_runs",
        "scan_benchmark_alerts",
      ])
    )
    .min(1)
    .max(10),
});

export async function POST(request: Request) {
  try {
    const payload = maintenancePayloadSchema.parse(await request.json());
    const results: Record<string, unknown> = {};

    for (const action of payload.actions) {
      if (action === "reconcile_stale_campaign_runs") {
        results[action] = await reconcileStaleCampaignRuns();
      } else if (action === "schedule_due_campaign_runs") {
        results[action] = await scheduleDueCampaignRuns();
      } else if (action === "fail_stale_benchmark_runs") {
        results[action] = await failStaleBenchmarkRuns();
      } else if (action === "fail_stale_crawl_runs") {
        results[action] = await failStaleCrawlRuns();
      } else if (action === "scan_benchmark_alerts") {
        results[action] = await scheduleBenchmarkDriftAlertScan({ force: true });
      }
    }

    return apiSuccess(
      {
        executedActions: payload.actions,
        results,
        executedAt: new Date().toISOString(),
      },
      { status: 202 }
    );
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export async function GET() {
  return apiError({
    status: 405,
    code: "method_not_allowed",
    message: "Use POST to execute runtime maintenance actions.",
  });
}
