import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getFraternityCrawlRequestCounts } from "@/lib/repositories/fraternity-crawl-request-repository";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const data = await getFraternityCrawlRequestCounts();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
