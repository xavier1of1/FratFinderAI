import { apiSuccess } from "@/lib/api-envelope";
import { authenticateOperator } from "@/lib/security/operator-access";

export async function GET(request: Request) {
  const operator = authenticateOperator(request);
  return apiSuccess({
    authenticated: Boolean(operator),
    actor: operator?.actor ?? null,
    role: operator?.role ?? null,
    authMode: operator?.authMode ?? null
  });
}
