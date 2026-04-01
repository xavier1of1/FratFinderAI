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

export type FraternityCrawlRequestStatus = "draft" | "queued" | "running" | "succeeded" | "failed" | "canceled";

export type FraternityCrawlRequestStage =
  | "discovery"
  | "awaiting_confirmation"
  | "crawl_run"
  | "enrichment"
  | "completed"
  | "failed";

export interface FraternityCrawlRequestConfig {
  fieldJobWorkers: number;
  fieldJobLimitPerCycle: number;
  maxEnrichmentCycles: number;
  pauseMs: number;
}

export interface FraternityDiscoveryCandidate {
  title: string;
  url: string;
  snippet: string;
  provider: string;
  rank: number;
  score: number;
}

export interface FraternityCrawlProgress {
  discovery?: {
    sourceUrl: string | null;
    sourceConfidence: number;
    confidenceTier: string;
    sourceProvenance?: "verified_registry" | "existing_source" | "search" | null;
    fallbackReason?: string | null;
    resolutionTrace?: Array<Record<string, unknown>>;
    candidates: FraternityDiscoveryCandidate[];
  };
  crawlRun?: {
    id: number | null;
    status: string | null;
    pagesProcessed: number;
    recordsSeen: number;
    recordsUpserted: number;
    reviewItemsCreated: number;
    fieldJobsCreated: number;
  };
  fields?: {
    find_website: Record<string, number>;
    find_email: Record<string, number>;
    find_instagram: Record<string, number>;
  };
  totals?: Record<string, number>;
}

export interface FraternityCrawlRequestEvent {
  id: number;
  requestId: string;
  eventType: string;
  message: string;
  payload: Record<string, unknown>;
  createdAt: string;
}

export interface FraternityCrawlRequest {
  id: string;
  fraternityName: string;
  fraternitySlug: string;
  sourceSlug: string | null;
  sourceUrl: string | null;
  sourceConfidence: number | null;
  status: FraternityCrawlRequestStatus;
  stage: FraternityCrawlRequestStage;
  scheduledFor: string;
  startedAt: string | null;
  finishedAt: string | null;
  priority: number;
  config: FraternityCrawlRequestConfig;
  progress: FraternityCrawlProgress;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
  events: FraternityCrawlRequestEvent[];
}
