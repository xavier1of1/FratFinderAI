import { NextRequest } from "next/server";

import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleFraternityCrawlRequest } from "@/lib/fraternity-crawl-request-runner";
import {
  appendFraternityCrawlRequestEvent,
  bumpQueuedFieldJobsForSource,
  getFraternityCrawlRequest,
  updateFraternityCrawlRequest
} from "@/lib/repositories/fraternity-crawl-request-repository";

export async function POST(_: NextRequest, context: { params: { id: string } }) {
  try {
    const id = context.params.id;
    const current = await getFraternityCrawlRequest(id);
    if (!current) {
      return apiError({ status: 404, code: "not_found", message: `Fraternity crawl request ${id} not found` });
    }

    if (!current.sourceSlug) {
      return apiError({
        status: 409,
        code: "missing_source",
        message: "Cannot expedite without a resolved source slug"
      });
    }

    const nowIso = new Date().toISOString();

    if (current.status !== "running") {
      await updateFraternityCrawlRequest({
        id,
        status: "queued",
        stage: current.stage === "awaiting_confirmation" ? "discovery" : current.stage,
        scheduledFor: nowIso,
        priority: 100,
        lastError: null
      });
    }

    const bumped = await bumpQueuedFieldJobsForSource(current.sourceSlug, 100);

    await appendFraternityCrawlRequestEvent({
      requestId: id,
      eventType: "request_expedited",
      message: "Request expedited by operator",
      payload: { bumpedQueuedFieldJobs: bumped }
    });

    await scheduleFraternityCrawlRequest(id);
    const refreshed = await getFraternityCrawlRequest(id);
    return apiSuccess(refreshed);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
