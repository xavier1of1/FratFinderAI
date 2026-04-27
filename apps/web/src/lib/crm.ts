import type { CrmChannel, CrmCampaignFilters } from "./types";

export interface CrmRecipientSeed {
  chapterId: string;
  fraternitySlug: string;
  fraternityName: string;
  chapterName: string;
  universityName: string | null;
  city: string | null;
  state: string | null;
  email: string | null;
  instagramUrl: string | null;
}

export interface CrmTemplateContext {
  chapterId: string;
  fraternitySlug: string;
  fraternityName: string;
  chapterName: string;
  universityName: string;
  city: string;
  state: string;
  email: string;
  instagramUrl: string;
  instagramHandle: string;
}

export function instagramHandleFromUrl(url: string | null | undefined): string {
  if (!url) {
    return "";
  }

  const normalized = url.trim();
  if (!normalized) {
    return "";
  }

  const match = normalized.match(/instagram\.com\/([^/?#]+)/i);
  return match?.[1]?.trim() ?? "";
}

export function buildCrmTemplateContext(seed: CrmRecipientSeed): CrmTemplateContext {
  return {
    chapterId: seed.chapterId,
    fraternitySlug: seed.fraternitySlug,
    fraternityName: seed.fraternityName,
    chapterName: seed.chapterName,
    universityName: seed.universityName ?? "",
    city: seed.city ?? "",
    state: seed.state ?? "",
    email: seed.email ?? "",
    instagramUrl: seed.instagramUrl ?? "",
    instagramHandle: instagramHandleFromUrl(seed.instagramUrl)
  };
}

const TOKEN_PATTERN = /\{([a-zA-Z0-9_]+)\}/g;

export function renderCrmTemplate(template: string, context: CrmTemplateContext): string {
  return template.replace(TOKEN_PATTERN, (_match, token: string) => {
    const normalized = token as keyof CrmTemplateContext;
    const value = context[normalized];
    return typeof value === "string" ? value : "";
  });
}

export function defaultCrmSubject(channel: CrmChannel): string | null {
  if (channel === "instagram") {
    return null;
  }
  return "Quick note for {chapterName} at {universityName}";
}

export function defaultCrmMessage(channel: CrmChannel): string {
  if (channel === "instagram") {
    return [
      "Hey {chapterName} at {universityName},",
      "",
      "We’re reaching out because we work with fraternity chapters on growth and operations tooling. If you’re the right person to chat with, I’d love to send a quick overview.",
      "",
      "Thanks,",
      "Frat Finder AI"
    ].join("\n");
  }

  return [
    "Hi {chapterName} team,",
    "",
    "I’m reaching out because we work with fraternity chapters on growth and operations tooling. If you’re the right person to talk with, I’d love to share a quick overview tailored to {universityName}.",
    "",
    "Best,",
    "Frat Finder AI"
  ].join("\n");
}

export function normalizeCrmFilters(input: Partial<CrmCampaignFilters> | null | undefined): CrmCampaignFilters {
  return {
    fraternitySlug: input?.fraternitySlug?.trim() || null,
    state: input?.state?.trim() || null,
    chapterStatus: input?.chapterStatus === "inactive" || input?.chapterStatus === "all" ? input.chapterStatus : "active",
    search: input?.search?.trim() || null,
    limit: typeof input?.limit === "number" && Number.isFinite(input.limit) ? Math.max(1, Math.min(500, Math.trunc(input.limit))) : 50
  };
}
