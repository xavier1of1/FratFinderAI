export type ReviewStatus = "open" | "triaged" | "resolved" | "ignored";

export interface ChapterListItem {
  id: string;
  fraternitySlug: string;
  slug: string;
  name: string;
  universityName: string | null;
  city: string | null;
  state: string | null;
  country: string;
  websiteUrl: string | null;
  instagramUrl: string | null;
  contactEmail: string | null;
  chapterStatus: string;
  fieldStates: Record<string, string>;
  updatedAt: string;
}

export interface CrawlRunListItem {
  id: number;
  sourceSlug: string | null;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  pagesProcessed: number;
  recordsSeen: number;
  recordsUpserted: number;
  reviewItemsCreated: number;
  fieldJobsCreated: number;
  lastError: string | null;
  strategyUsed: string | null;
  pageLevelConfidence: number | null;
  llmCallsUsed: number;
}

export interface ReviewItemListItem {
  id: string;
  sourceSlug: string | null;
  chapterSlug: string | null;
  itemType: string;
  status: string;
  reason: string;
  extractionNotes: string | null;
  triageNotes: string | null;
  createdAt: string;
  updatedAt: string;
  lastActor: string | null;
  lastAction: string | null;
  lastActionAt: string | null;
}

export interface FieldJobListItem {
  id: string;
  chapterSlug: string;
  fieldName: string;
  status: string;
  terminalFailure: boolean;
  claimedBy: string | null;
  attempts: number;
  maxAttempts: number;
  scheduledAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  lastError: string | null;
}

export interface ReviewItemAuditLog {
  id: number;
  reviewItemId: string;
  actor: string;
  action: string;
  fromStatus: ReviewStatus;
  toStatus: ReviewStatus;
  notes: string | null;
  createdAt: string;
}
