import { NextRequest } from "next/server";
import { z } from "zod";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { deleteChapterRecords, enqueueChapterReruns } from "@/lib/repositories/chapter-repository";

const actionSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("rerun"),
    chapterIds: z.array(z.string().uuid()).min(1),
    fieldNames: z.array(z.enum(["find_website", "find_email", "find_instagram"])).min(1)
  }),
  z.object({
    action: z.literal("delete"),
    chapterIds: z.array(z.string().uuid()).min(1)
  })
]);

export async function POST(request: NextRequest) {
  try {
    const payload = actionSchema.parse(await request.json());

    if (payload.action === "rerun") {
      const result = await enqueueChapterReruns({
        chapterIds: payload.chapterIds,
        fieldNames: payload.fieldNames
      });
      return apiSuccess(result, { status: 202 });
    }

    const result = await deleteChapterRecords(payload.chapterIds);
    return apiSuccess(result);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
