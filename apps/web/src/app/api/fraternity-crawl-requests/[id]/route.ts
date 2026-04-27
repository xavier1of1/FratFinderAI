import { NextRequest } from "next/server";
import { z } from "zod";

import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleDueFraternityCrawlRequests, scheduleFraternityCrawlRequest } from "@/lib/fraternity-crawl-request-runner";
import { evaluateSourceUrl } from "@/lib/source-selection";
import {
  appendFraternityCrawlRequestEvent,
  getFraternityCrawlRequest,
  updateFraternityCrawlRequest,
  upsertFraternityRecord,
  upsertSourceRecord
} from "@/lib/repositories/fraternity-crawl-request-repository";

export const dynamic = "force-dynamic";

const patchSchema = z.object({
  action: z.enum(["confirm", "cancel", "reschedule"]),
  sourceUrl: z.string().url().optional(),
  scheduledFor: z.string().datetime().optional()
});

function slugify(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function getDiscoveredSourceUrl(current: Awaited<ReturnType<typeof getFraternityCrawlRequest>>): string | null {
  const candidate = current?.progress?.discovery?.candidates?.[0];
  if (candidate && typeof candidate.url === "string" && candidate.url.trim()) {
    return candidate.url.trim();
  }
  const discovered = current?.progress?.discovery?.sourceUrl;
  if (typeof discovered === "string" && discovered.trim()) {
    return discovered.trim();
  }
  return null;
}

export async function GET(_: NextRequest, context: { params: { id: string } }) {
  try {
    const run = await getFraternityCrawlRequest(context.params.id);
    if (!run) {
      return apiError({ status: 404, code: "not_found", message: `Fraternity crawl request ${context.params.id} not found` });
    }

    return apiSuccess(run);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export async function PATCH(request: NextRequest, context: { params: { id: string } }) {
  try {
    const id = context.params.id;
    const payload = patchSchema.parse(await request.json());
    const current = await getFraternityCrawlRequest(id);

    if (!current) {
      return apiError({ status: 404, code: "not_found", message: `Fraternity crawl request ${id} not found` });
    }

    if (payload.action === "cancel") {
      await updateFraternityCrawlRequest({
        id,
        status: "canceled",
        stage: "failed",
        finishedAtNow: true,
        lastError: "Canceled by operator"
      });
      await appendFraternityCrawlRequestEvent({
        requestId: id,
        eventType: "request_canceled",
        message: "Request canceled by operator"
      });
      const refreshed = await getFraternityCrawlRequest(id);
      return apiSuccess(refreshed);
    }

    if (payload.action === "reschedule") {
      if (!payload.scheduledFor) {
        return apiError({ status: 400, code: "invalid_request", message: "scheduledFor is required for reschedule action" });
      }

      await updateFraternityCrawlRequest({
        id,
        scheduledFor: payload.scheduledFor,
        status: current.status === "draft" ? "draft" : "queued",
        stage: current.status === "draft" ? current.stage : "discovery",
        finishedAtNow: false
      });
      await appendFraternityCrawlRequestEvent({
        requestId: id,
        eventType: "request_rescheduled",
        message: "Request rescheduled",
        payload: { scheduledFor: payload.scheduledFor }
      });
      await scheduleDueFraternityCrawlRequests();
      const refreshed = await getFraternityCrawlRequest(id);
      return apiSuccess(refreshed);
    }

    // confirm
    let sourceUrl = payload.sourceUrl ?? current.sourceUrl ?? getDiscoveredSourceUrl(current);
    let sourceSlug = current.sourceSlug;
    let sourceConfidence =
      current.sourceConfidence ??
      Number(current.progress?.discovery?.sourceConfidence ?? current.progress?.discovery?.candidates?.[0]?.score ?? 0.6);
    const confirmedAt = new Date().toISOString();

    if (!sourceUrl) {
      return apiError({
        status: 409,
        code: "missing_source",
        message: "Confirm requires a sourceUrl when discovery confidence was not high enough. Pick a discovery candidate or paste a source URL."
      });
    }

    if (sourceUrl) {
      const fraternityName = current.fraternityName;
      const fraternitySlug = current.fraternitySlug || slugify(fraternityName);
      const fraternityRecord = await upsertFraternityRecord({
        slug: fraternitySlug,
        name: fraternityName,
        nicAffiliated: true
      });

      if (!sourceSlug) {
        sourceSlug = `${fraternityRecord.slug}-main`;
      }

      await upsertSourceRecord({
        fraternityId: fraternityRecord.id,
        slug: sourceSlug,
        baseUrl: new URL(sourceUrl).origin,
        listPath: sourceUrl,
        sourceType: "html_directory",
        parserKey: "directory_v1",
        active: true,
        metadata: {
          confirmedByOperator: true,
          confirmedAt,
          sourceQuality: evaluateSourceUrl(sourceUrl)
        }
      });
    }

    const sourceQuality = evaluateSourceUrl(sourceUrl);
    sourceConfidence = Math.max(sourceConfidence ?? 0, sourceQuality.score);
    const confidenceTier = sourceConfidence >= 0.8 ? "high" : sourceConfidence >= 0.6 ? "medium" : "low";
    const currentDiscovery = current.progress?.discovery;
    const nextProgress = {
      ...(current.progress ?? {}),
      discovery: {
        sourceUrl,
        sourceConfidence,
        confidenceTier,
        sourceProvenance: currentDiscovery?.sourceProvenance ?? null,
        fallbackReason: currentDiscovery?.fallbackReason ?? null,
        sourceQuality,
        selectedCandidateRationale: currentDiscovery?.selectedCandidateRationale,
        resolutionTrace: currentDiscovery?.resolutionTrace ?? [],
        candidates: currentDiscovery?.candidates ?? [],
        confirmedByOperator: true,
        confirmedAt
      },
      analytics: {
        ...((current.progress?.analytics as Record<string, unknown> | undefined) ?? {}),
        sourceQuality: {
          recoveryAttempts: current.progress?.analytics?.sourceQuality?.recoveryAttempts ?? 0,
          recoveredFromUrl: current.progress?.analytics?.sourceQuality?.recoveredFromUrl ?? null,
          recoveredToUrl: current.progress?.analytics?.sourceQuality?.recoveredToUrl ?? null,
          sourceRejectedCount: current.progress?.analytics?.sourceQuality?.sourceRejectedCount ?? 0,
          sourceRecoveredCount: current.progress?.analytics?.sourceQuality?.sourceRecoveredCount ?? 0,
          zeroChapterPrevented: current.progress?.analytics?.sourceQuality?.zeroChapterPrevented ?? 0,
          sourcePreservedCount: current.progress?.analytics?.sourceQuality?.sourcePreservedCount ?? 0,
          ...sourceQuality,
          confirmedByOperator: true,
          confirmedAt
        }
      }
    };

    await updateFraternityCrawlRequest({
      id,
      sourceSlug,
      sourceUrl,
      sourceConfidence,
      status: "queued",
      stage: "discovery",
      scheduledFor: payload.scheduledFor ?? current.scheduledFor,
      priority: 0,
      progress: nextProgress,
      clearFinishedAt: true,
      lastError: null
    });

    await appendFraternityCrawlRequestEvent({
      requestId: id,
      eventType: "request_confirmed",
      message: "Request confirmed and queued",
      payload: {
        sourceSlug,
        sourceUrl,
        sourceQuality,
        scheduledFor: payload.scheduledFor ?? current.scheduledFor
      }
    });

    await scheduleDueFraternityCrawlRequests();
    const refreshed = await getFraternityCrawlRequest(id);
    if (refreshed && refreshed.status === "queued" && new Date(refreshed.scheduledFor).getTime() <= Date.now()) {
      await scheduleFraternityCrawlRequest(refreshed.id);
    }
    return apiSuccess(await getFraternityCrawlRequest(id));
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
