import { describe, expect, it } from "vitest";

import type { FraternitySourceDiscoveryResult } from "./fraternity-discovery";
import { evaluateSourceUrl, optimizeDiscoveredSource } from "./source-selection";

describe("evaluateSourceUrl", () => {
  it("marks member and alumni paths as weak", () => {
    const evaluation = evaluateSourceUrl("https://members.sigmachi.org/alumnigroups");

    expect(evaluation.isWeak).toBe(true);
    expect(evaluation.score).toBeLessThan(0.45);
    expect(evaluation.reasons.some((reason) => reason.includes("weak:alumni"))).toBe(true);
  });

  it("rewards chapter directory style paths", () => {
    const evaluation = evaluateSourceUrl("https://sigmachi.org/chapters/");

    expect(evaluation.isWeak).toBe(false);
    expect(evaluation.score).toBeGreaterThan(0.55);
  });

  it("rewards chapter locator and chapter roll style paths", () => {
    const locator = evaluateSourceUrl("https://apa1906.net/chapter-locator/");
    const chapterRoll = evaluateSourceUrl("https://aepi.org/about/chapter-roll/");

    expect(locator.isWeak).toBe(false);
    expect(locator.score).toBeGreaterThan(0.6);
    expect(chapterRoll.isWeak).toBe(false);
    expect(chapterRoll.score).toBeGreaterThan(0.6);
  });

  it("scores generic about pages lower than chapter directories on the same host", () => {
    const about = evaluateSourceUrl("https://www.thetachi.org/about");
    const chapters = evaluateSourceUrl("https://www.thetachi.org/chapters");

    expect(about.score).toBeLessThan(chapters.score);
    expect(about.reasons.some((reason) => reason.startsWith("generic_info:"))).toBe(true);
  });

  it("scores shop pages lower than chapter directories", () => {
    const store = evaluateSourceUrl("https://www.shopthetachi.com/");
    const chapters = evaluateSourceUrl("https://www.thetachi.org/chapters");

    expect(store.score).toBeLessThan(chapters.score);
    expect(store.reasons.some((reason) => reason.startsWith("weak:"))).toBe(true);
  });
});

describe("optimizeDiscoveredSource", () => {
  it("upgrades a weak selected source to a stronger chapter candidate", () => {
    const discovery: FraternitySourceDiscoveryResult = {
      fraternityName: "Sigma Chi",
      fraternitySlug: "sigma-chi",
      selectedUrl: "https://members.sigmachi.org/alumnigroups",
      selectedConfidence: 0.95,
      confidenceTier: "high",
      sourceProvenance: "verified_registry",
      fallbackReason: null,
      resolutionTrace: [],
      candidates: [
        {
          title: "Sigma Chi chapters",
          url: "https://sigmachi.org/chapters/",
          snippet: "Find a chapter and view undergraduate chapter listings.",
          provider: "search",
          rank: 1,
          score: 0.82
        }
      ]
    };

    const optimized = optimizeDiscoveredSource(discovery);

    expect(optimized.selectedUrl).toBe("https://sigmachi.org/chapters/");
    expect(optimized.fallbackReason).toContain("upgraded_source_selection");
    expect(optimized.resolutionTrace.some((entry) => entry.step === "optimized_source_selection")).toBe(true);
  });

  it("does not upgrade a good chapter source to an unrelated generic chapter listing", () => {
    const discovery: FraternitySourceDiscoveryResult = {
      fraternityName: "Theta Xi",
      fraternitySlug: "theta-xi",
      selectedUrl: "https://www.thetaxi.org/chapters-and-colonies/",
      selectedConfidence: 0.85,
      confidenceTier: "high",
      sourceProvenance: "search",
      fallbackReason: "existing_source_invalid",
      resolutionTrace: [],
      candidates: [
        {
          title: "Chapter Listing - Kappa Kappa Psi",
          url: "https://www.kkpsi.org/about/chapters-districts/chapter-listing-2/",
          snippet: "Find chapter listings by school and district.",
          provider: "search",
          rank: 1,
          score: 0.69,
        },
        {
          title: "Theta Xi Fraternity Join",
          url: "https://www.thetaxi.org/join/",
          snippet: "Join Theta Xi Fraternity and learn more about the national organization.",
          provider: "search",
          rank: 2,
          score: 0.99,
        }
      ]
    };

    const optimized = optimizeDiscoveredSource(discovery);

    expect(optimized.selectedUrl).toBe("https://www.thetaxi.org/chapters-and-colonies/");
    expect(optimized.selectedUrl).not.toContain("kkpsi.org");
  });

  it("upgrades a generic about page to a same-fraternity chapter directory candidate", () => {
    const discovery: FraternitySourceDiscoveryResult = {
      fraternityName: "Theta Chi",
      fraternitySlug: "theta-chi",
      selectedUrl: "https://www.thetachi.org/about",
      selectedConfidence: 0.99,
      confidenceTier: "high",
      sourceProvenance: "search",
      fallbackReason: null,
      resolutionTrace: [],
      candidates: [
        {
          title: "Chapters | Theta Chi",
          url: "https://www.thetachi.org/chapters",
          snippet: "Browse active chapters and their chapter pages.",
          provider: "search",
          rank: 1,
          score: 0.88,
        }
      ]
    };

    const optimized = optimizeDiscoveredSource(discovery);

    expect(optimized.selectedUrl).toBe("https://www.thetachi.org/chapters");
    expect(optimized.fallbackReason).toContain("upgraded_source_selection");
  });
});
