import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { failStaleCrawlRuns } from "@/lib/repositories/crawl-run-repository";
import {
  getAgentOpsSummary,
  listChapterEvidence,
  listChapterSearchRuns,
  listProvisionalChapters,
  listRequestGraphRuns
} from "@/lib/repositories/agent-ops-repository";

export async function GET(request: NextRequest) {
  try {
    await failStaleCrawlRuns();
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "50");
    const safeLimit = Number.isNaN(limit) ? 50 : Math.min(Math.max(limit, 1), 200);

    const [summary, graphRuns, provisionalChapters, evidence, chapterSearchRuns] = await Promise.all([
      getAgentOpsSummary(),
      listRequestGraphRuns(safeLimit),
      listProvisionalChapters(safeLimit),
      listChapterEvidence(Math.min(safeLimit * 2, 300)),
      listChapterSearchRuns(safeLimit)
    ]);

    return apiSuccess({
      summary,
      graphRuns,
      provisionalChapters,
      evidence,
      chapterSearchRuns
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
