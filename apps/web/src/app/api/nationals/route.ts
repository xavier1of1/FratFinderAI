import { NextRequest } from "next/server";

import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { listNationalProfiles } from "@/lib/repositories/nationals-profile-repository";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = Number(searchParams.get("limit") ?? "200");
    const safeLimit = Number.isNaN(limit) ? 200 : Math.min(Math.max(limit, 1), 500);

    const items = await listNationalProfiles(safeLimit);
    return apiSuccess(items);
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
