import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listAdaptiveEpochMetrics } from "@/lib/repositories/adaptive-repository";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "60");
    const runtimeMode = searchParams.get("runtimeMode");
    const cohortLabel = searchParams.get("cohortLabel");
    const policyVersion = searchParams.get("policyVersion");

    const rows = await listAdaptiveEpochMetrics({
      limit: Number.isNaN(limit) ? 60 : limit,
      runtimeMode,
      cohortLabel,
      policyVersion,
    });

    return apiSuccess(rows);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
