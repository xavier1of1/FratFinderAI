import { NextRequest } from "next/server";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { createEvaluationJob } from "@/lib/repositories/evaluation-job-repository";
import { createCampaignRun, listCampaignRuns, reconcileStaleCampaignRuns } from "@/lib/repositories/campaign-run-repository";
import type { CampaignRun, CampaignRunConfig } from "@/lib/types";

export const dynamic = "force-dynamic";

const campaignPayloadSchema = z.object({
  name: z.string().trim().min(1).max(160).optional(),
  scheduledFor: z.string().datetime().optional(),
  config: z
    .object({
      targetCount: z.coerce.number().int().min(3).max(50).optional(),
      controlCount: z.coerce.number().int().min(0).max(10).optional(),
      activeConcurrency: z.coerce.number().int().min(1).max(12).optional(),
      maxDurationMinutes: z.coerce.number().int().min(15).max(240).optional(),
      checkpointIntervalMs: z.coerce.number().int().min(10_000).max(600_000).optional(),
      tuningIntervalMs: z.coerce.number().int().min(30_000).max(900_000).optional(),
      itemPollIntervalMs: z.coerce.number().int().min(5_000).max(120_000).optional(),
      preflightRequired: z.boolean().optional(),
      autoTuningEnabled: z.boolean().optional(),
      controlFraternitySlugs: z.array(z.string().trim().min(1).max(160)).max(20).optional(),
      programMode: z.enum(["standard", "v4_rl_improvement"]).optional(),
      runtimeMode: z.enum(["legacy", "adaptive_shadow", "adaptive_assisted", "adaptive_primary"]).optional(),
      fieldJobRuntimeMode: z.enum(["legacy", "langgraph_shadow", "langgraph_primary"]).optional(),
      frozenSourceSlugs: z.array(z.string().trim().min(1).max(160)).max(30).optional(),
      trainingRounds: z.coerce.number().int().min(1).max(6).optional(),
      epochsPerRound: z.coerce.number().int().min(1).max(8).optional(),
      trainingSourceBatchSize: z.coerce.number().int().min(1).max(20).optional(),
      evalSourceBatchSize: z.coerce.number().int().min(1).max(20).optional(),
      trainingCommandTimeoutMinutes: z.coerce.number().int().min(5).max(180).optional(),
      checkpointPromotionEnabled: z.boolean().optional(),
      queueStallThresholdMinutes: z.coerce.number().int().min(5).max(120).optional(),
      reviewWindowDays: z.coerce.number().int().min(1).max(90).optional()
    })
    .optional()
});

function formatDefaultCampaignName(config?: Partial<CampaignRunConfig>): string {
  const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19);
  const size = config?.targetCount ?? 20;
  if (config?.programMode === "v4_rl_improvement") {
    return `V4 RL improvement program ${timestamp}`;
  }
  return `${size}-fraternity campaign ${timestamp}`;
}

function toCampaignListItem(campaign: CampaignRun): CampaignRun {
  const runtimeActive =
    Boolean(campaign.runtimeWorkerId) &&
    (!campaign.runtimeLeaseExpiresAt || new Date(campaign.runtimeLeaseExpiresAt).getTime() >= Date.now());
  return {
    ...campaign,
    runtimeActive,
    telemetry: {
      providerHealth: campaign.telemetry.providerHealth ?? null,
      providerHealthHistory: [],
      activeConcurrency: campaign.telemetry.activeConcurrency,
      lastCheckpointAt: campaign.telemetry.lastCheckpointAt ?? null,
      lastTuneAt: campaign.telemetry.lastTuneAt ?? null,
      runtimeNotes: [],
      cohortManifest: [],
      activePolicyVersion: campaign.telemetry.activePolicyVersion ?? null,
      activePolicySnapshotId: campaign.telemetry.activePolicySnapshotId ?? null,
      promotionDecisions: [],
      queueStallAlert: campaign.telemetry.queueStallAlert ?? null,
      delayedRewardHealth: campaign.telemetry.delayedRewardHealth ?? null,
      reviewReasonDrift: [],
      acceptanceGate: campaign.telemetry.acceptanceGate ?? null,
      baselineSnapshot: null,
      finalSnapshot: null,
      programPhase: campaign.telemetry.programPhase,
      programStartedAt: campaign.telemetry.programStartedAt ?? null,
    },
    items: [],
    events: [],
  };
}

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "50");
    const data = await listCampaignRuns(Number.isNaN(limit) ? 50 : Math.min(Math.max(limit, 1), 200));
    return apiSuccess(data.map((campaign) => toCampaignListItem(campaign)));
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export async function POST(request: NextRequest) {
  try {
    await reconcileStaleCampaignRuns();
    const payload = campaignPayloadSchema.parse(await request.json());

    const created = await createCampaignRun({
      name: payload.name?.trim() || formatDefaultCampaignName(payload.config),
      config: payload.config,
      scheduledFor: payload.scheduledFor
    });

    await createEvaluationJob({
      jobKind: "campaign_run",
      campaignRunId: created.id,
      isolationMode: "shared_live_observed",
      payload: {
        scheduledFor: created.scheduledFor,
        programMode: created.config.programMode,
      },
    });

    const data = (await listCampaignRuns(1)).find((item) => item.id === created.id) ?? created;
    return apiSuccess(data, { status: 202 });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
