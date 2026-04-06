export function normalizeInstagramUrl(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const raw = value.trim();
  if (!raw) {
    return null;
  }
  let normalized = raw;
  if (normalized.startsWith("@")) {
    normalized = normalized.slice(1);
  }
  if (!/^https?:\/\//i.test(normalized)) {
    if (/instagram\.com\//i.test(normalized)) {
      normalized = `https://${normalized.replace(/^\/+/, "")}`;
    } else {
      normalized = `https://www.instagram.com/${normalized}`;
    }
  }
  const match = normalized.match(/(?:https?:\/\/)?(?:www\.)?instagram\.com\/([A-Za-z0-9_.-]+)/i);
  if (!match) {
    return null;
  }
  const handle = match[1]?.replace(/^@+/, "").split("/")[0]?.split("?")[0]?.split("#")[0];
  if (!handle) {
    return null;
  }
  if (["p", "reel", "tv", "stories", "explore", "accounts", "mailto"].includes(handle.toLowerCase())) {
    return null;
  }
  return `https://www.instagram.com/${handle}`;
}

export function instagramHandleFromUrl(value: string | null | undefined): string | null {
  const normalized = normalizeInstagramUrl(value);
  if (!normalized) {
    return null;
  }
  const match = normalized.match(/instagram\.com\/([A-Za-z0-9_.-]+)/i);
  return match?.[1] ?? null;
}
