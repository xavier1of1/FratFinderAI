import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listCrawlRuns } from "@/lib/repositories/crawl-run-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

async function listRunsHandler(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "100");

    const data = await listCrawlRuns(Number.isNaN(limit) ? 100 : Math.min(Math.max(limit, 1), 500));
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess("dashboard_read", { targetType: "crawl_run" }, listRunsHandler);
