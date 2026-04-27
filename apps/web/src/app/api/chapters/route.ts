import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getChapterListMetadata, listChapters } from "@/lib/repositories/chapter-repository";

function parseBoolean(value: string | null): boolean {
  if (!value) {
    return false;
  }
  const normalized = value.trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const q = searchParams.get("q") ?? "";
    const limit = Number(searchParams.get("limit") ?? "50");
    const offset = Number(searchParams.get("offset") ?? "0");
    const includeMeta = parseBoolean(searchParams.get("includeMeta"));

    const pagination = {
      search: q,
      limit: Number.isNaN(limit) ? 50 : Math.min(Math.max(limit, 1), 5000),
      offset: Number.isNaN(offset) ? 0 : Math.max(offset, 0)
    };

    if (includeMeta) {
      const [items, metadata] = await Promise.all([
        listChapters(pagination),
        getChapterListMetadata({ search: q })
      ]);
      return apiSuccess({
        items,
        totalCount: metadata.totalCount,
        fraternitySlugs: metadata.fraternitySlugs,
        stateOptions: metadata.stateOptions,
        chapterStatuses: metadata.chapterStatuses,
        withWebsiteCount: metadata.withWebsiteCount,
        withInstagramCount: metadata.withInstagramCount,
        withEmailCount: metadata.withEmailCount
      });
    }

    const data = await listChapters(pagination);
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
