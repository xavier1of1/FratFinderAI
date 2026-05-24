import { logoutResponse } from "@/lib/security/operator-access";

export async function POST() {
  return logoutResponse();
}
