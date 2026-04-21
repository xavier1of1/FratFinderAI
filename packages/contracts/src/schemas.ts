import { z } from "zod";

export const crawlRunStatusSchema = z.enum([
  "pending",
  "running",
  "succeeded",
  "failed",
  "partial"
]);

export const reviewStatusSchema = z.enum(["open", "triaged", "resolved", "ignored"]);
export const fieldJobStatusSchema = z.enum(["queued", "running", "done", "failed"]);

export const canonicalChapterSchema = z.object({
  fraternitySlug: z.string().min(1),
  sourceSlug: z.string().min(1),
  externalId: z.string().min(1).nullable().optional(),
  slug: z.string().min(1),
  name: z.string().min(1),
  universityName: z.string().min(1).nullable().optional(),
  city: z.string().min(1).nullable().optional(),
  state: z.string().min(1).nullable().optional(),
  country: z.string().min(1).default("USA"),
  websiteUrl: z.string().url().nullable().optional(),
  chapterStatus: z.enum(["active", "inactive", "unknown"]).default("active"),
  missingOptionalFields: z.array(z.string()).default([]),
  fieldStates: z.record(z.string()).optional()
});

export const chapterProvenanceSchema = z.object({
  sourceSlug: z.string().min(1),
  sourceUrl: z.string().url(),
  fieldName: z.string().min(1),
  fieldValue: z.string().nullable().optional(),
  sourceSnippet: z.string().nullable().optional(),
  confidence: z.number().min(0).max(1).default(1)
});

export const reviewItemPayloadSchema = z.object({
  itemType: z.string().min(1),
  reason: z.string().min(1),
  sourceSlug: z.string().nullable().optional(),
  chapterSlug: z.string().nullable().optional(),
  extractionNotes: z.string().optional(),
  payload: z.record(z.unknown()).default({})
});

export const fieldJobPayloadSchema = z.object({
  chapterSlug: z.string().min(1),
  fieldName: z.string().min(1),
  sourceSlug: z.string().min(1),
  payload: z.record(z.unknown()).default({})
});

export const chapterStatusDecisionSchema = z.object({
  finalStatus: z.enum(["active", "inactive", "unknown", "review"]),
  schoolRecognitionStatus: z.enum([
    "recognized",
    "probationary_recognition",
    "active_under_conduct_sanction",
    "seeking_recognition",
    "interim_suspension",
    "suspended",
    "closed",
    "dismissed",
    "expelled",
    "unrecognized",
    "banned_no_greek_life",
    "not_found_on_conclusive_roster",
    "unknown"
  ]),
  nationalStatus: z.enum([
    "active",
    "associate",
    "colony",
    "inactive",
    "dormant",
    "closed",
    "not_listed_on_active_only_directory",
    "not_listed_on_all_status_directory",
    "unknown"
  ]).default("unknown"),
  reasonCode: z.string().min(1),
  confidence: z.number().min(0).max(1),
  evidenceIds: z.array(z.string()).default([]),
  decisionTrace: z.record(z.unknown()).default({}),
  conflictFlags: z.array(z.string()).default([]),
  reviewRequired: z.boolean().default(false)
}).superRefine((value, ctx) => {
  if ((value.finalStatus === "active" || value.finalStatus === "inactive") && value.evidenceIds.length === 0) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "active/inactive decisions require evidenceIds"
    });
  }
});

export type CrawlRunStatus = z.infer<typeof crawlRunStatusSchema>;
export type ReviewStatus = z.infer<typeof reviewStatusSchema>;
export type FieldJobStatus = z.infer<typeof fieldJobStatusSchema>;
export type CanonicalChapter = z.infer<typeof canonicalChapterSchema>;
export type ChapterProvenance = z.infer<typeof chapterProvenanceSchema>;
export type ReviewItemPayload = z.infer<typeof reviewItemPayloadSchema>;
export type FieldJobPayload = z.infer<typeof fieldJobPayloadSchema>;
export type ChapterStatusDecision = z.infer<typeof chapterStatusDecisionSchema>;
