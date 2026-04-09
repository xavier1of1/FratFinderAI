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

export interface ChapterListResponse {
  items: ChapterListItem[];
  totalCount: number;
  fraternitySlugs: string[];
  stateOptions: string[];
  chapterStatuses: string[];
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
  chapterSearch?: {
    sourceClass?: string | null;
    candidatesExtracted?: number;
    candidatesRejected?: number;
    canonicalChaptersCreated?: number;
    provisionalChaptersCreated?: number;
    nationalTargetsFollowed?: number;
    institutionalTargetsFollowed?: number;
    chapterOwnedTargetsSkipped?: number;
    broaderWebTargetsFollowed?: number;
    chapterSearchWallTimeMs?: number;
    rejectionReasonCounts?: Record<string, number>;
    coverageState?: string | null;
  } | null;
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
export type BenchmarkIsolationMode = "shared_live_observed" | "strict_live_isolated";

export interface BenchmarkRunConfig {
  fieldName: BenchmarkFieldName;
  sourceSlug: string | null;
  workers: number;
  limitPerCycle: number;
  cycles: number;
  pauseMs: number;
  crawlRuntimeMode?: "legacy" | "adaptive_shadow" | "adaptive_assisted" | "adaptive_primary";
  fieldJobRuntimeMode?: "legacy" | "langgraph_shadow" | "langgraph_primary";
  fieldJobGraphDurability?: "exit" | "async" | "sync";
  runAdaptiveCrawlBeforeCycles?: boolean;
  isolationMode?: BenchmarkIsolationMode;
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
  businessStatus?: "progressed" | "no_business_progress";
  jobsPerMinute: number;
  avgCycleMs: number;
  queueDepthStart: number;
  queueDepthEnd: number;
  queueDepthDelta: number;
  invalidBlocked?: number;
  repairableBlocked?: number;
  repairPromoted?: number;
  reconciledHistorical?: number;
  actionableQueueRemaining?: number;
  preconditions?: Record<string, unknown>;
  isolationMode?: BenchmarkIsolationMode;
  contaminationStatus?: "isolated" | "shared_live";
}

export interface BenchmarkShadowDiff {
  id: number;
  benchmarkRunId: string;
  cycle: number;
  runtimeMode: string;
  observedJobs: number;
  decisionMismatchCount: number;
  statusMismatchCount: number;
  mismatchRate: number;
  details: Record<string, unknown>;
  createdAt: string;
}

export interface BenchmarkGateCheck {
  label: string;
  value: string;
  target: string;
  passed: boolean;
}

export interface BenchmarkGateReport {
  benchmarkId: string;
  benchmarkName: string;
  baselineBenchmarkId: string | null;
  baselineBenchmarkName: string | null;
  checks: BenchmarkGateCheck[];
}

export interface BenchmarkRunListItem {
  id: string;
  name: string;
  status: BenchmarkStatus;
  benchmarkKind?: "queue" | "campaign";
  campaignRunId?: string | null;
  runtimeWorkerId?: string | null;
  runtimeLeaseExpiresAt?: string | null;
  runtimeLastHeartbeatAt?: string | null;
  fieldName: BenchmarkFieldName;
  sourceSlug: string | null;
  config: BenchmarkRunConfig;
  summary: BenchmarkRunSummary | null;
  samples: BenchmarkCycleSample[];
  shadowDiffs?: BenchmarkShadowDiff[];
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

export interface BenchmarkAlert {
  id: number;
  benchmarkRunId: string | null;
  alertType: string;
  severity: "info" | "warning" | "critical";
  status: "open" | "resolved";
  message: string;
  fingerprint: string | null;
  payload: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  resolvedAt: string | null;
}

export interface BenchmarkAlertSummary {
  openTotal: number;
  resolvedTotal: number;
  openInfo: number;
  openWarning: number;
  openCritical: number;
  resolvedLast24h: number;
  lastUpdatedAt: string;
}
export type FraternityCrawlRequestStatus = "draft" | "queued" | "running" | "succeeded" | "failed" | "canceled";

export type FraternityCrawlRequestStage =
  | "discovery"
  | "awaiting_confirmation"
  | "crawl_run"
  | "purge_inactive_schools"
  | "enrichment"
  | "completed"
  | "failed";

export interface FraternityCrawlRequestConfig {
  fieldJobWorkers: number;
  fieldJobLimitPerCycle: number;
  maxEnrichmentCycles: number;
  pauseMs: number;
  crawlPolicyVersion?: string | null;
}

export interface FraternityCrawlSourceQuality {
  score: number;
  isWeak: boolean;
  isBlocked?: boolean;
  reasons: string[];
  recoveryAttempts: number;
  recoveredFromUrl?: string | null;
  recoveredToUrl?: string | null;
  sourceRejectedCount?: number;
  sourceRecoveredCount?: number;
  zeroChapterPrevented?: number;
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
  runtimeFallbackCount?: number;
  zeroChapterPrevented?: number;
  queueBurnRate?: number;
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
    sourceQuality?: {
      score: number;
      isWeak: boolean;
      isBlocked?: boolean;
      reasons: string[];
    } | null;
    selectedCandidateRationale?: string | null;
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
  graph?: {
    requestGraphRunId?: number | null;
    runtimeMode?: string | null;
    workerId?: string | null;
    activeNode?: string | null;
  };
  provisional?: {
    evaluated?: boolean;
    autoPromoted?: number;
    reviewRequired?: number;
    rejected?: number;
    remaining?: number;
  };
  chapterSearch?: {
    sourceClass?: string | null;
    candidatesExtracted?: number;
    candidatesRejected?: number;
    canonicalChaptersCreated?: number;
    provisionalChaptersCreated?: number;
    nationalTargetsFollowed?: number;
    institutionalTargetsFollowed?: number;
    chapterOwnedTargetsSkipped?: number;
    broaderWebTargetsFollowed?: number;
    chapterSearchWallTimeMs?: number;
    rejectionReasonCounts?: Record<string, number>;
    coverageState?: string | null;
  };
  chapterValidity?: {
    invalidCount?: number;
    repairableCount?: number;
    provisionalCount?: number;
    canonicalValidCount?: number;
    invalidReasonCounts?: Record<string, number>;
    repairReasonCounts?: Record<string, number>;
    sourceInvaliditySaturated?: boolean;
    contactAdmission?: {
      blocked_invalid?: number;
      blocked_repairable?: number;
      admitted_canonical?: number;
    };
  };
  queueTriage?: {
    invalidCancelled?: number;
    deferredLongCooldown?: number;
    repairQueued?: number;
    actionableRetained?: number;
    sourceInvaliditySaturated?: boolean;
    purgedInactiveChapters?: number;
    purgedBannedSchoolChapters?: number;
  };
  chapterRepair?: {
    queued?: number;
    running?: number;
    promotedToCanonical?: number;
    downgradedToProvisional?: number;
    confirmedInvalid?: number;
    repairExhausted?: number;
    reconciledHistorical?: number;
  };
  contactResolution?: {
    queuedActionable?: number;
    queuedDeferred?: number;
    processed?: number;
    requeued?: number;
    reviewRequired?: number;
    terminalNoSignal?: number;
    providerDegraded?: number;
    autoWritten?: number;
    writesByField?: Record<string, number>;
    actionableRemaining?: number;
    blockedInvalid?: number;
    blockedRepairable?: number;
    reconciledHistorical?: number;
    rejectionReasonCounts?: Record<string, number>;
  };
}

export interface RequestGraphRun {
  id: number;
  requestId: string;
  workerId: string;
  runtimeMode: string;
  status: string;
  activeNode: string | null;
  summary: Record<string, unknown>;
  metadata: Record<string, unknown>;
  errorMessage: string | null;
  createdAt: string;
  updatedAt: string;
  finishedAt: string | null;
  fraternityName: string | null;
  fraternitySlug: string | null;
  sourceSlug: string | null;
  requestStage: string | null;
  requestStatus: string | null;
}

export interface ProvisionalChapter {
  id: string;
  fraternityId: string;
  fraternitySlug: string | null;
  sourceSlug: string | null;
  requestId: string | null;
  promotedChapterId: string | null;
  slug: string;
  name: string;
  universityName: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
  websiteUrl: string | null;
  instagramUrl: string | null;
  contactEmail: string | null;
  status: string;
  promotionReason: string | null;
  evidencePayload: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface OpsAlert {
  id: string;
  alertScope: "benchmark" | "campaign" | "queue" | "repair" | "provider" | "system";
  alertType: string;
  severity: "info" | "warning" | "critical";
  status: "open" | "resolved";
  benchmarkRunId: string | null;
  campaignRunId: string | null;
  requestId: string | null;
  sourceSlug: string | null;
  message: string;
  fingerprint: string | null;
  payload: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  resolvedAt: string | null;
}

export interface ChapterEvidence {
  id: string;
  chapterId: string | null;
  chapterSlug: string | null;
  fraternitySlug: string | null;
  sourceSlug: string | null;
  requestId: string | null;
  crawlRunId: number | null;
  fieldName: string;
  candidateValue: string | null;
  confidence: number | null;
  trustTier: string | null;
  evidenceStatus: string | null;
  sourceUrl: string | null;
  sourceSnippet: string | null;
  provider: string | null;
  query: string | null;
  relatedWebsiteUrl: string | null;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface AgentOpsSummary {
  requestQueueQueued: number;
  requestQueueRunning: number;
  requestAwaitingConfirmation: number;
  requestCompleted: number;
  graphRunsTotal: number;
  graphRunsRunning: number;
  graphRunsPaused: number;
  graphRunsFailed: number;
  graphRunsSucceeded: number;
  fieldJobsQueued: number;
  fieldJobsActionable: number;
  fieldJobsRunning: number;
  fieldJobsDeferred: number;
  fieldJobsBlockedInvalid: number;
  fieldJobsBlockedRepairable: number;
  chapterRepairQueued: number;
  chapterRepairRunning: number;
  chapterRepairCompleted: number;
  chapterRepairHistoricalReconciled: number;
  fieldJobsTerminalNoSignal: number;
  fieldJobsReviewRequired: number;
  fieldJobsUpdated: number;
  provisionalOpen: number;
  provisionalPromoted: number;
  provisionalReview: number;
  provisionalRejected: number;
  evidenceTotal: number;
  evidenceReview: number;
  evidenceWrite: number;
  chapterSearchRuns: number;
  chapterSearchCanonical: number;
  chapterSearchProvisional: number;
  chapterSearchChapterOwnedSkipped: number;
  chapterValidityInvalid: number;
  chapterValidityRepairable: number;
  chapterValidityBlockedInvalid: number;
  chapterValidityBlockedRepairable: number;
  opsAlertsOpen: number;
  opsAlertsCritical: number;
  opsAlertsWarning: number;
  opsAlertsResolvedLast24h: number;
  opsAlertsOldestOpenMinutes: number;
  provisionalOldestOpenHours: number;
}

export interface ChapterSearchRun {
  id: number;
  sourceSlug: string | null;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  runtimeMode: string | null;
  strategyUsed: string | null;
  stopReason: string | null;
  pagesProcessed: number;
  recordsSeen: number;
  recordsUpserted: number;
  reviewItemsCreated: number;
  fieldJobsCreated: number;
  sourceClass: string | null;
  coverageState: string | null;
  candidatesExtracted: number;
  candidatesRejected: number;
  canonicalChaptersCreated: number;
  provisionalChaptersCreated: number;
  nationalTargetsFollowed: number;
  institutionalTargetsFollowed: number;
  chapterOwnedTargetsSkipped: number;
  broaderWebTargetsFollowed: number;
  chapterSearchWallTimeMs: number;
  rejectionReasonCounts: Record<string, number>;
  invalidCount: number;
  repairableCount: number;
  canonicalValidCount: number;
  provisionalCount: number;
  sourceInvaliditySaturated: boolean;
  invalidReasonCounts: Record<string, number>;
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
  runtimeWorkerId?: string | null;
  runtimeLeaseExpiresAt?: string | null;
  runtimeLastHeartbeatAt?: string | null;
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

export type CampaignProgramMode = "standard" | "v4_rl_improvement";

export interface CampaignPromotionDecision {
  round: number;
  stagedPolicyVersion: string;
  snapshotId: number | null;
  promoted: boolean;
  reason: string;
  balancedScore: number;
  queueQueued: number;
  placeholderReviewCount: number;
  overlongReviewCount: number;
  createdAt: string;
}

export interface CampaignQueueStallAlert {
  active: boolean;
  since: string | null;
  reason: string | null;
  queuedDepth: number;
  lastProcessedTotal: number;
}

export interface CampaignReviewReasonDrift {
  reason: string;
  baselineCount: number;
  latestCount: number;
  delta: number;
}

export interface CampaignDelayedRewardHealth {
  delayedRewardEventCount: number;
  delayedRewardTotal: number;
  placeholderReviewCount: number;
  overlongReviewCount: number;
  guardrailHitRate: number;
  validMissingCount: number;
  verifiedWebsiteCount: number;
  topDelayedActions: AdaptiveDelayedAttribution[];
}

export interface CampaignAcceptanceGateCheck {
  label: string;
  value: string;
  target: string;
  passed: boolean;
}

export interface CampaignAcceptanceGate {
  passed: boolean;
  checks: CampaignAcceptanceGateCheck[];
  baselineSnapshot?: Record<string, unknown> | null;
  finalSnapshot?: Record<string, unknown> | null;
}

export interface AdaptivePolicySnapshot {
  id: number;
  policyVersion: string;
  runtimeMode: string;
  featureSchemaVersion: string;
  metrics: Record<string, unknown>;
  createdAt: string;
}

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
  programMode?: CampaignProgramMode;
  runtimeMode?: "legacy" | "adaptive_shadow" | "adaptive_assisted" | "adaptive_primary";
  fieldJobRuntimeMode?: "legacy" | "langgraph_shadow" | "langgraph_primary";
  frozenSourceSlugs?: string[];
  trainingRounds?: number;
  epochsPerRound?: number;
  trainingSourceBatchSize?: number;
  evalSourceBatchSize?: number;
  trainingCommandTimeoutMinutes?: number;
  checkpointPromotionEnabled?: boolean;
  queueStallThresholdMinutes?: number;
  reviewWindowDays?: number;
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
  businessStatus?: "progressed" | "no_business_progress";
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
  cohortManifest?: string[];
  activePolicyVersion?: string | null;
  activePolicySnapshotId?: number | null;
  promotionDecisions?: CampaignPromotionDecision[];
  queueStallAlert?: CampaignQueueStallAlert | null;
  delayedRewardHealth?: CampaignDelayedRewardHealth | null;
  reviewReasonDrift?: CampaignReviewReasonDrift[];
  acceptanceGate?: CampaignAcceptanceGate | null;
  baselineSnapshot?: Record<string, unknown> | null;
  finalSnapshot?: Record<string, unknown> | null;
  programPhase?: "baseline" | "training" | "live_campaign" | "completed";
  programStartedAt?: string | null;
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
  runtimeWorkerId?: string | null;
  runtimeLeaseExpiresAt?: string | null;
  runtimeLastHeartbeatAt?: string | null;
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



export interface AdaptiveEpochMetric {
  id: number;
  epoch: number;
  policyVersion: string;
  runtimeMode: string;
  trainSources: string[];
  evalSources: string[];
  kpis: Record<string, number>;
  deltas: Record<string, number>;
  slopes: Record<string, number>;
  cohortLabel: string;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface AdaptiveActionInsight {
  actionType: string;
  count: number;
  avgScore: number;
  avgRisk: number;
  recordsExtracted: number;
}

export interface AdaptiveDelayedAttribution {
  actionType: string;
  count: number;
  avgReward: number;
  totalReward: number;
}

export interface AdaptiveInsights {
  actionLeaderboard: AdaptiveActionInsight[];
  delayedAttribution: AdaptiveDelayedAttribution[];
  guardrailHitRate: number;
  totalPages: number;
  guardrailPages: number;
  validMissingCount: number;
  verifiedWebsiteCount: number;
  delayedRewardEventCount: number;
  delayedRewardTotal: number;
  placeholderReviewCount: number;
  overlongReviewCount: number;
  topReviewReasons: Array<{ reason: string; count: number }>;
}

export interface FieldJobGraphRunListItem {
  id: number;
  workerId: string;
  runtimeMode: string;
  sourceSlug: string | null;
  fieldName: string | null;
  requestedLimit: number;
  status: string;
  summary: Record<string, unknown>;
  errorMessage: string | null;
  createdAt: string;
  updatedAt: string;
  finishedAt: string | null;
  eventCount: number;
  decisionCount: number;
}

export interface FieldJobGraphEventItem {
  id: number;
  runId: number;
  jobId: string | null;
  attempt: number | null;
  nodeName: string;
  phase: string;
  status: string;
  latencyMs: number;
  metricsDelta: Record<string, unknown>;
  diagnostics: Record<string, unknown>;
  createdAt: string;
}

export interface FieldJobGraphDecisionItem {
  id: number;
  runId: number;
  jobId: string;
  attempt: number;
  fieldName: string;
  decisionStatus: string;
  confidence: number | null;
  candidateKind: string | null;
  candidateValue: string | null;
  reasonCodes: string[];
  writeAllowed: boolean;
  requiresReview: boolean;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface FieldJobGraphRunDetail {
  run: FieldJobGraphRunListItem;
  events: FieldJobGraphEventItem[];
  decisions: FieldJobGraphDecisionItem[];
}



