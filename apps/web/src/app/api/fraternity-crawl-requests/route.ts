import { NextRequest } from "next/server";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { discoverFraternitySource } from "@/lib/fraternity-discovery";
import { scheduleDueFraternityCrawlRequests, scheduleFraternityCrawlRequest } from "@/lib/fraternity-crawl-request-runner";
import {
  appendFraternityCrawlRequestEvent,
  createFraternityCrawlRequest,
  listFraternityCrawlRequests,
  reconcileStaleFraternityCrawlRequests,
  upsertFraternityRecord,
  upsertSourceRecord
} from "@/lib/repositories/fraternity-crawl-request-repository";

const payloadSchema = z.object({
  fraternityName: z.string().trim().min(2).max(120),
  scheduledFor: z.string().datetime().optional(),
  config: z
    .object({
      fieldJobWorkers: z.coerce.number().int().min(1).max(16).optional(),
      fieldJobLimitPerCycle: z.coerce.number().int().min(1).max(500).optional(),
      maxEnrichmentCycles: z.coerce.number().int().min(1).max(200).optional(),
      pauseMs: z.coerce.number().int().min(0).max(30_000).optional()
    })
    .optional()
});

function slugify(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export async function GET(request: NextRequest) {
  try {
    await reconcileStaleFraternityCrawlRequests();
    await scheduleDueFraternityCrawlRequests();

    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "100");
    const data = await listFraternityCrawlRequests(Number.isNaN(limit) ? 100 : Math.min(Math.max(limit, 1), 500));
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export async function POST(request: NextRequest) {
  try {
    await reconcileStaleFraternityCrawlRequests();

    const payload = payloadSchema.parse(await request.json());
    const discovery = await discoverFraternitySource(payload.fraternityName);

    const sourceUrl = discovery.selectedUrl;
    const sourceConfidence = discovery.selectedConfidence;
    const confidenceTier = discovery.confidenceTier;

    const fraternityName = discovery.fraternityName || payload.fraternityName.trim();
    const fraternitySlug = discovery.fraternitySlug || slugify(fraternityName);

    const fraternityRecord = await upsertFraternityRecord({
      slug: fraternitySlug,
      name: fraternityName,
      nicAffiliated: true
    });

    let sourceSlug: string | null = null;
    if (sourceUrl) {
      const baseUrl = new URL(sourceUrl).origin;
      sourceSlug = `${fraternityRecord.slug}-main`;
      await upsertSourceRecord({
        fraternityId: fraternityRecord.id,
        slug: sourceSlug,
        baseUrl,
        listPath: sourceUrl,
        sourceType: "html_directory",
        parserKey: "directory_v1",
        active: true,
        metadata: {
          discovery: {
            selectedUrl: sourceUrl,
            selectedConfidence: sourceConfidence,
            confidenceTier
          }
        }
      });
    }

    const shouldAutoQueue = (confidenceTier === "high" || confidenceTier === "medium") && Boolean(sourceUrl);
    const status = shouldAutoQueue ? "queued" : "draft";
    const stage = shouldAutoQueue ? "discovery" : "awaiting_confirmation";

    const created = await createFraternityCrawlRequest({
      fraternityName,
      fraternitySlug,
      sourceSlug,
      sourceUrl,
      sourceConfidence,
      status,
      stage,
      scheduledFor: payload.scheduledFor ?? new Date().toISOString(),
      priority: 0,
      config: payload.config,
      progress: {
        discovery: {
          sourceUrl,
          sourceConfidence,
          confidenceTier,
          candidates: discovery.candidates
        }
      },
      lastError: !shouldAutoQueue ? "Low-confidence source discovery; confirm source before running." : null
    });

    await appendFraternityCrawlRequestEvent({
      requestId: created.id,
      eventType: "request_created",
      message: `Request created for ${fraternityName}`,
      payload: {
        confidenceTier,
        sourceUrl,
        sourceSlug
      }
    });

    if (status === "queued") {
      await appendFraternityCrawlRequestEvent({
        requestId: created.id,
        eventType: "request_queued",
        message: "Request queued for staged crawl execution",
        payload: {
          scheduledFor: created.scheduledFor
        }
      });
      if (new Date(created.scheduledFor).getTime() <= Date.now()) {
        await scheduleFraternityCrawlRequest(created.id);
      }
    }

    return apiSuccess(created, { status: 202 });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
