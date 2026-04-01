import { NextRequest } from "next/server";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { updateChapterRecord } from "@/lib/repositories/chapter-repository";

const updateSchema = z.object({
  name: z.string().trim().min(1).max(200),
  universityName: z.string().trim().max(200).nullable().optional(),
  city: z.string().trim().max(120).nullable().optional(),
  state: z.string().trim().max(32).nullable().optional(),
  chapterStatus: z.string().trim().min(1).max(50),
  websiteUrl: z.string().trim().url().nullable().optional().or(z.literal("")),
  instagramUrl: z.string().trim().url().nullable().optional().or(z.literal("")),
  contactEmail: z.string().trim().email().nullable().optional().or(z.literal(""))
});

function normalizeNullable(value: string | null | undefined): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  const normalized = value.trim();
  return normalized.length ? normalized : null;
}

export async function PATCH(request: NextRequest, context: { params: { id: string } }) {
  try {
    const payload = updateSchema.parse(await request.json());
    const updated = await updateChapterRecord({
      id: context.params.id,
      name: payload.name,
      universityName: normalizeNullable(payload.universityName),
      city: normalizeNullable(payload.city),
      state: normalizeNullable(payload.state),
      chapterStatus: payload.chapterStatus,
      websiteUrl: normalizeNullable(payload.websiteUrl),
      instagramUrl: normalizeNullable(payload.instagramUrl),
      contactEmail: normalizeNullable(payload.contactEmail)
    });

    if (!updated) {
      throw new Error(`Chapter ${context.params.id} not found`);
    }

    return apiSuccess(updated);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
