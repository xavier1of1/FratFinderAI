import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listReviewItems } from "@/lib/repositories/review-item-repository";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "100");

    const data = await listReviewItems(Number.isNaN(limit) ? 100 : Math.min(Math.max(limit, 1), 500));
    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
