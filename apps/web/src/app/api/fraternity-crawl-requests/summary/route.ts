import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getFraternityCrawlRequestCounts } from "@/lib/repositories/fraternity-crawl-request-repository";
import { withReadOnlyOperatorAccess } from "@/lib/security/operator-access";

export const dynamic = "force-dynamic";

async function getFraternityCrawlRequestSummaryHandler() {
  try {
    const data = await getFraternityCrawlRequestCounts();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}

export const GET = withReadOnlyOperatorAccess("dashboard_read", { targetType: "fraternity_crawl_request_summary" }, getFraternityCrawlRequestSummaryHandler);
