import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getAdaptiveInsights } from "@/lib/repositories/adaptive-repository";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const sourceSlugsRaw = searchParams.get("sourceSlugs");
    const sourceSlugs = sourceSlugsRaw
      ? sourceSlugsRaw
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean)
      : undefined;
    const runtimeMode = searchParams.get("runtimeMode");
    const windowDays = Number(searchParams.get("windowDays") ?? "7");
    const limit = Number(searchParams.get("limit") ?? "25");

    const data = await getAdaptiveInsights({
      sourceSlugs,
      runtimeMode,
      windowDays: Number.isNaN(windowDays) ? 7 : windowDays,
      limit: Number.isNaN(limit) ? 25 : limit,
    });

    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
