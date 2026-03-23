import { apiSuccess } from "@/lib/api-envelope";

export async function GET() {
  return apiSuccess({
    ok: true,
    service: "web",
    probe: "liveness",
    timestamp: new Date().toISOString()
  });
}