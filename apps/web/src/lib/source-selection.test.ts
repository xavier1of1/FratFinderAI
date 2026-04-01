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
});
