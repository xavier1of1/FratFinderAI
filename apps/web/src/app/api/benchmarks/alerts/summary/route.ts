import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getBenchmarkAlertSummary } from "@/lib/repositories/benchmark-repository";

export async function GET() {
  try {
    const summary = await getBenchmarkAlertSummary();
    return apiSuccess(summary);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
