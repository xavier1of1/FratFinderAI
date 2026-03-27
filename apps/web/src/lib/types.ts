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

export type BenchmarkStatus = "queued" | "running" | "succeeded" | "failed";

export type BenchmarkFieldName = "find_website" | "find_email" | "find_instagram" | "all";

export interface BenchmarkRunConfig {
  fieldName: BenchmarkFieldName;
  sourceSlug: string | null;
  workers: number;
  limitPerCycle: number;
  cycles: number;
  pauseMs: number;
}

export interface BenchmarkCycleSample {
  cycle: number;
  startedAt: string;
  durationMs: number;
  processed: number;
  requeued: number;
  failedTerminal: number;
  queued: number;
  running: number;
  done: number;
  failed: number;
}

export interface BenchmarkRunSummary {
  elapsedMs: number;
  cyclesCompleted: number;
  totalProcessed: number;
  totalRequeued: number;
  totalFailedTerminal: number;
  jobsPerMinute: number;
  avgCycleMs: number;
  queueDepthStart: number;
  queueDepthEnd: number;
  queueDepthDelta: number;
}

export interface BenchmarkRunListItem {
  id: string;
  name: string;
  status: BenchmarkStatus;
  fieldName: BenchmarkFieldName;
  sourceSlug: string | null;
  config: BenchmarkRunConfig;
  summary: BenchmarkRunSummary | null;
  samples: BenchmarkCycleSample[];
  startedAt: string | null;
  finishedAt: string | null;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface BenchmarkQueueSnapshot {
  queued: number;
  running: number;
  done: number;
  failed: number;
  total: number;
}