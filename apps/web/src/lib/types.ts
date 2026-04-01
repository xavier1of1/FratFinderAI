export type ReviewStatus = "open" | "triaged" | "resolved" | "ignored";

export interface ChapterListItem {
  id: string;
  fraternitySlug: string;
  sourceSlug: string | null;
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

export type ChapterFieldName = "find_website" | "find_email" | "find_instagram";

export interface ChapterActionResult {
  affectedCount: number;
  requestedCount: number;
  skippedCount?: number;
  missingSourceCount?: number;
}

export interface ChapterMapStateSummary {
  stateCode: string;
  chapterCount: number;
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
  runtimeMode: string | null;
  stopReason: string | null;
  crawlSessionCount: number;
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
  candidateValue: string | null;
  confidence: number | null;
  sourceUrl: string | null;
  query: string | null;
  rejectionSummary: {
    topReasons: Array<{ reason: string; count: number }>;
    uniqueReasons: number;
    totalRejections: number;
  } | null;
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
  crawlRuntimeMode?: "legacy" | "adaptive_shadow" | "adaptive_assisted" | "adaptive_primary";
  runAdaptiveCrawlBeforeCycles?: boolean;
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
  benchmarkKind?: "queue" | "campaign";
  campaignRunId?: string | null;
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

export interface FraternityCrawlSourceQuality {
  score: number;
  isWeak: boolean;
  reasons: string[];
  recoveryAttempts: number;
  recoveredFromUrl?: string | null;
  recoveredToUrl?: string | null;
}

export interface FraternityCrawlEnrichmentAnalytics {
  adaptiveMaxEnrichmentCycles: number;
  effectiveFieldJobWorkers: number;
  effectiveFieldJobLimitPerCycle: number;
  cyclesCompleted: number;
  lowProgressCycles: number;
  degradedCycleCount: number;
  queueAtStart: number;
  queueRemaining: number;
  budgetStrategy: string;
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
  analytics?: {
    sourceQuality?: FraternityCrawlSourceQuality;
    enrichment?: FraternityCrawlEnrichmentAnalytics;
  };
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

export type CampaignRunStatus = "draft" | "queued" | "running" | "succeeded" | "failed" | "canceled";
export type CampaignRunItemStatus =
  | "planned"
  | "request_created"
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "skipped"
  | "canceled";

export interface CampaignRunConfig {
  targetCount: number;
  controlCount: number;
  activeConcurrency: number;
  maxDurationMinutes: number;
  checkpointIntervalMs: number;
  tuningIntervalMs: number;
  itemPollIntervalMs: number;
  preflightRequired: boolean;
  autoTuningEnabled: boolean;
  controlFraternitySlugs: string[];
}

export interface CampaignFailureHistogramEntry {
  reason: string;
  count: number;
}

export interface CampaignProviderHealthSnapshot {
  healthy: boolean;
  successRate: number;
  probes: number;
  successes: number;
  minSuccessRate: number;
  providerHealth: Record<string, Record<string, number>>;
}

export interface CampaignProviderHealthHistoryPoint extends CampaignProviderHealthSnapshot {
  timestamp: string;
  activeConcurrency: number;
  queueDepth: number;
}

export interface CampaignScorecard {
  baselineTotalChapters: number;
  baselineWebsitesFound: number;
  baselineEmailsFound: number;
  baselineInstagramsFound: number;
  baselineChaptersWithAnyContact: number;
  baselineChaptersWithAllThree: number;
  chaptersDiscovered: number;
  fieldJobsCreated: number;
  processedJobs: number;
  requeuedJobs: number;
  failedTerminalJobs: number;
  reviewItemsCreated: number;
  websitesFound: number;
  emailsFound: number;
  instagramsFound: number;
  chaptersWithAnyContact: number;
  chaptersWithAllThree: number;
  sourceNativeYield: number;
  searchEfficiency: number;
  retryEfficiency: number;
  confidenceQuality: number;
  providerResilience: number;
  queueEfficiency: number;
  providerAttempts: Record<string, number>;
  failureHistogram: CampaignFailureHistogramEntry[];
}

export interface CampaignRunSummary {
  targetCount: number;
  itemCount: number;
  completedCount: number;
  failedCount: number;
  skippedCount: number;
  activeCount: number;
  anyContactSuccessRate: number;
  allThreeSuccessRate: number;
  websiteCoverageRate: number;
  emailCoverageRate: number;
  instagramCoverageRate: number;
  jobsPerMinute: number;
  queueDepthStart: number;
  queueDepthEnd: number;
  queueDepthDelta: number;
  totalProcessed: number;
  totalRequeued: number;
  totalFailedTerminal: number;
  durationMs: number;
  checkpointCount: number;
}

export interface CampaignRunEvent {
  id: number;
  campaignRunId: string;
  eventType: string;
  message: string;
  payload: Record<string, unknown>;
  createdAt: string;
}

export interface CampaignRunItem {
  id: string;
  campaignRunId: string;
  fraternityName: string;
  fraternitySlug: string;
  requestId: string | null;
  cohort: "new" | "control";
  status: CampaignRunItemStatus;
  selectionReason: string | null;
  scorecard: CampaignScorecard;
  notes: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface CampaignRunTelemetry {
  providerHealth?: CampaignProviderHealthSnapshot | null;
  providerHealthHistory?: CampaignProviderHealthHistoryPoint[];
  activeConcurrency?: number;
  lastCheckpointAt?: string | null;
  lastTuneAt?: string | null;
  runtimeNotes?: string[];
}

export interface CampaignCohortSummary {
  cohort: "new" | "control";
  itemCount: number;
  completedCount: number;
  failedCount: number;
  anyContactSuccessRate: number;
  allThreeSuccessRate: number;
  websiteCoverageRate: number;
  emailCoverageRate: number;
  instagramCoverageRate: number;
  avgJobsPerItem: number;
}

export interface CampaignReport {
  campaignId: string;
  campaignName: string;
  generatedAt: string;
  summary: CampaignRunSummary;
  providerHealthHistory: CampaignProviderHealthHistoryPoint[];
  cohortComparison: CampaignCohortSummary[];
  topFailureReasons: Array<{ reason: string; count: number }>;
  topSuccessfulHabits: Array<{ label: string; value: number }>;
  recommendations: string[];
}

export interface CampaignRun {
  id: string;
  name: string;
  status: CampaignRunStatus;
  runtimeActive?: boolean;
  scheduledFor: string;
  startedAt: string | null;
  finishedAt: string | null;
  config: CampaignRunConfig;
  summary: CampaignRunSummary;
  telemetry: CampaignRunTelemetry;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
  items: CampaignRunItem[];
  events: CampaignRunEvent[];
}


