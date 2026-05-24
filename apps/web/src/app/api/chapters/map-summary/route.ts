import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listChapterMapSummary } from "@/lib/repositories/chapter-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

async function getChapterMapSummaryHandler() {
  try {
    const data = await listChapterMapSummary();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess("dashboard_read", { targetType: "chapter_map_summary" }, getChapterMapSummaryHandler);
