import { NextRequest } from "next/server";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { createCrmCampaign, listCrmCampaigns } from "@/lib/repositories/crm-repository";

export const dynamic = "force-dynamic";

const payloadSchema = z.object({
  name: z.string().trim().min(1).max(160),
  channel: z.enum(["email", "instagram"]),
  deliveryMode: z.enum(["operator", "outlook"]).optional(),
  subjectTemplate: z.string().trim().max(240).nullable().optional().or(z.literal("")),
  messageTemplate: z.string().trim().min(1).max(10_000).optional(),
  filters: z
    .object({
      fraternitySlug: z.string().trim().max(160).nullable().optional().or(z.literal("")),
      state: z.string().trim().max(10).nullable().optional().or(z.literal("")),
      chapterStatus: z.enum(["active", "inactive", "all"]).optional(),
      search: z.string().trim().max(160).nullable().optional().or(z.literal("")),
      limit: z.coerce.number().int().min(1).max(500).optional()
    })
    .optional()
});

export async function GET(request: NextRequest) {
  try {
    const limit = Number(request.nextUrl.searchParams.get("limit") ?? "50");
    const campaigns = await listCrmCampaigns(Number.isNaN(limit) ? 50 : limit);
    return apiSuccess(campaigns);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export async function POST(request: Request) {
  try {
    const payload = payloadSchema.parse(await request.json());
    const campaign = await createCrmCampaign({
      name: payload.name,
      channel: payload.channel,
      deliveryMode: payload.deliveryMode,
      subjectTemplate: payload.subjectTemplate || null,
      messageTemplate: payload.messageTemplate,
      filters: payload.filters
    });
    return apiSuccess(campaign, { status: 201 });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
