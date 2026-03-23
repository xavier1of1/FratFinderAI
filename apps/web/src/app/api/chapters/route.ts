import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listChapters } from "@/lib/repositories/chapter-repository";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const q = searchParams.get("q") ?? "";
    const limit = Number(searchParams.get("limit") ?? "50");
    const offset = Number(searchParams.get("offset") ?? "0");

    const data = await listChapters({
      search: q,
      limit: Number.isNaN(limit) ? 50 : Math.min(Math.max(limit, 1), 500),
      offset: Number.isNaN(offset) ? 0 : Math.max(offset, 0)
    });

    return apiSuccess(data);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
