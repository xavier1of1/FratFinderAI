import { apiError, apiSuccess } from "@/lib/api-envelope";
import { getDbPool } from "@/lib/db";

export async function GET() {
  try {
    const dbPool = getDbPool();
    await dbPool.query("SELECT 1");
    return apiSuccess({
      ok: true,
      service: "web",
      probes: {
        liveness: "ok",
        readiness: "ok"
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    return apiError({
      status: 503,
      code: "service_unavailable",
      message: "Health check failed.",
      details: error instanceof Error ? error.message : String(error)
    });
  }
}