import { describe, expect, it } from "vitest";

import { buildCrmTemplateContext, defaultCrmMessage, defaultCrmSubject, instagramHandleFromUrl, normalizeCrmFilters, renderCrmTemplate } from "./crm";

describe("crm helpers", () => {
  it("extracts instagram handles from profile urls", () => {
    expect(instagramHandleFromUrl("https://www.instagram.com/sigmachiuchicago/")).toBe("sigmachiuchicago");
    expect(instagramHandleFromUrl(null)).toBe("");
  });

  it("renders template tokens from chapter context", () => {
    const context = buildCrmTemplateContext({
      chapterId: "chapter-1",
      fraternitySlug: "sigma-chi",
      fraternityName: "Sigma Chi",
      chapterName: "Gamma Beta",
      universityName: "State University",
      city: "Boston",
      state: "MA",
      email: "chapter@example.edu",
      instagramUrl: "https://www.instagram.com/gammabeta/"
    });

    expect(renderCrmTemplate("Hi {chapterName} at {universityName}", context)).toBe("Hi Gamma Beta at State University");
    expect(renderCrmTemplate("IG {instagramHandle}", context)).toBe("IG gammabeta");
  });

  it("normalizes CRM filters safely", () => {
    expect(normalizeCrmFilters({ limit: 900, chapterStatus: "active", fraternitySlug: " sigma-chi " })).toEqual({
      fraternitySlug: "sigma-chi",
      state: null,
      chapterStatus: "active",
      search: null,
      limit: 500
    });
  });

  it("provides default templates for both channels", () => {
    expect(defaultCrmSubject("email")).toContain("{chapterName}");
    expect(defaultCrmMessage("instagram")).toContain("Frat Finder AI");
  });
});
