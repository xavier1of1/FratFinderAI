import { describe, expect, it } from "vitest";
import {
  canonicalChapterSchema,
  chapterProvenanceSchema,
  fieldJobPayloadSchema,
  reviewItemPayloadSchema
} from "../src/schemas";

describe("contract schemas", () => {
  it("accepts canonical chapter payload", () => {
    const payload = {
      fraternitySlug: "beta-theta-pi",
      sourceSlug: "beta-theta-pi-main",
      slug: "ohio-state",
      name: "Beta Lambda",
      country: "USA",
      chapterStatus: "active",
      missingOptionalFields: [],
      fieldStates: {
        website_url: "found"
      }
    };

    const parsed = canonicalChapterSchema.parse(payload);
    expect(parsed.slug).toBe("ohio-state");
    expect(parsed.fieldStates?.website_url).toBe("found");
  });

  it("rejects malformed chapter payload", () => {
    expect(() => canonicalChapterSchema.parse({})).toThrow();
  });

  it("validates provenance payload", () => {
    const payload = {
      sourceSlug: "beta-theta-pi-main",
      sourceUrl: "https://example.org/chapters",
      fieldName: "name",
      confidence: 1
    };

    expect(chapterProvenanceSchema.parse(payload).fieldName).toBe("name");
  });

  it("validates review and field-job payloads", () => {
    const review = reviewItemPayloadSchema.parse({
      itemType: "unsupported_source",
      reason: "No adapter registered",
      extractionNotes: "Detected a locator shell with no reachable backing API.",
      payload: {}
    });

    const fieldJob = fieldJobPayloadSchema.parse({
      chapterSlug: "ohio-state",
      fieldName: "websiteUrl",
      sourceSlug: "beta-theta-pi-main",
      payload: {}
    });

    expect(review.itemType).toBe("unsupported_source");
    expect(review.extractionNotes).toContain("locator shell");
    expect(fieldJob.fieldName).toBe("websiteUrl");
  });
});
