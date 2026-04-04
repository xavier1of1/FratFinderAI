import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listFieldJobGraphRuns } from "@/lib/repositories/field-job-graph-repository";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "50");
    const sourceSlug = searchParams.get("sourceSlug");
    const fieldName = searchParams.get("fieldName");
    const runtimeMode = searchParams.get("runtimeMode");

    const data = await listFieldJobGraphRuns({
      limit: Number.isFinite(limit) ? limit : 50,
      sourceSlug: sourceSlug?.trim() ? sourceSlug.trim() : null,
      fieldName: fieldName?.trim() ? fieldName.trim() : null,
      runtimeMode: runtimeMode?.trim() ? runtimeMode.trim() : null,
    });

    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
