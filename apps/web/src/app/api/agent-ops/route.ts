import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import {
  getAgentOpsSummary,
  listChapterEvidence,
  listChapterSearchRuns,
  listOpsAlertsForAgentOps,
  listProvisionalChapters,
  listRequestGraphRuns
} from "@/lib/repositories/agent-ops-repository";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "50");
    const safeLimit = Number.isNaN(limit) ? 50 : Math.min(Math.max(limit, 1), 200);

    const [summary, graphRuns, provisionalChapters, evidence, chapterSearchRuns, opsAlerts] = await Promise.all([
      getAgentOpsSummary(),
      listRequestGraphRuns(safeLimit),
      listProvisionalChapters(safeLimit),
      listChapterEvidence(Math.min(safeLimit * 2, 300)),
      listChapterSearchRuns(safeLimit),
      listOpsAlertsForAgentOps(safeLimit)
    ]);

    return apiSuccess({
      summary,
      graphRuns,
      provisionalChapters,
      evidence,
      chapterSearchRuns,
      opsAlerts
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
