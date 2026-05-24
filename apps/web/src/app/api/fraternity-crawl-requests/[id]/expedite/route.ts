import { NextRequest } from "next/server";

import { apiError, apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { scheduleFraternityCrawlRequest } from "@/lib/fraternity-crawl-request-runner";
import { withOperatorAccess } from "@/lib/security/operator-access";
import {
  appendFraternityCrawlRequestEvent,
  bumpQueuedFieldJobsForSource,
  getFraternityCrawlRequest,
  updateFraternityCrawlRequest
} from "@/lib/repositories/fraternity-crawl-request-repository";

async function expediteRequestHandler(_: Request, context: { params: { id: string } }) {
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
        stage: current.stage === "awaiting_confirmation" || current.stage === "promotion_recovery_needed" ? "discovery" : current.stage,
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

export const POST = withOperatorAccess(
  ["operator", "admin"],
  "crawl_request_expedite",
  (_request, context: { params: { id: string } }) => ({ targetType: "fraternity_crawl_request", targetId: context.params.id }),
  expediteRequestHandler
);
