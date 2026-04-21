import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getBenchmarkRunCounts } from "@/lib/repositories/benchmark-repository";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const data = await getBenchmarkRunCounts();
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
