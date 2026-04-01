import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listChapterMapSummary } from "@/lib/repositories/chapter-repository";

export async function GET() {
  try {
    const data = await listChapterMapSummary();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
