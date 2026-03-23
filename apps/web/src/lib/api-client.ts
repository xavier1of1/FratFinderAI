import { headers } from "next/headers";

function getBaseUrl(): string {
  if (typeof window !== "undefined") {
    return "";
  }

  try {
    const headerStore = headers();
    const host = headerStore.get("x-forwarded-host") ?? headerStore.get("host");
    const protocol = headerStore.get("x-forwarded-proto") ?? "http";
    if (host) {
      return `${protocol}://${host}`;
    }
  } catch {
    // Fall through to environment-based fallback for non-request contexts.
  }

  return process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:3000";
}

export async function fetchFromApi<T>(path: string): Promise<T> {
  const response = await fetch(`${getBaseUrl()}${path}`, { cache: "no-store" });
  const payload = (await response.json()) as
    | { success: true; data: T }
    | { success: false; error: { code: string; message: string; requestId: string } };

  if (!response.ok || !payload.success) {
    const errorMessage = payload.success
      ? `API request failed for ${path}: ${response.status}`
      : `${payload.error.code}: ${payload.error.message} (${payload.error.requestId})`;
    throw new Error(errorMessage);
  }
  return payload.data;
}
