import { beforeEach, describe, expect, it, vi } from "vitest";

const reconcileStaleCampaignRuns = vi.fn(async () => 0);
const scheduleDueCampaignRuns = vi.fn(async () => 0);
const scheduleCampaignRun = vi.fn(async () => true);
const isCampaignRunActive = vi.fn(() => false);
const countActiveRuntimeWorkersByLane = vi.fn(async () => ({ campaign: 0, benchmark: 0, evaluation: 0 }));

const failStaleBenchmarkRuns = vi.fn(async () => 0);
const createBenchmarkRun = vi.fn(async () => ({
  id: "benchmark-created",
  name: "benchmark",
  status: "queued",
  fieldName: "find_email",
  sourceSlug: null,
  config: {
    fieldName: "find_email",
    sourceSlug: null,
    workers: 1,
    limitPerCycle: 1,
    cycles: 1,
    pauseMs: 0,
    crawlRuntimeMode: "adaptive_assisted",
    fieldJobRuntimeMode: "legacy",
    fieldJobGraphDurability: "sync",
    runAdaptiveCrawlBeforeCycles: false,
    isolationMode: "shared_live_observed",
  },
  summary: null,
  samples: [],
  startedAt: null,
  finishedAt: null,
  lastError: null,
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
}));
const getBenchmarkRun = vi.fn(async () => null);
const listBenchmarkRuns = vi.fn(async () => []);
const getBenchmarkRunCounts = vi.fn(async () => ({
  total: 1,
  queued: 0,
  running: 0,
  succeeded: 1,
  failed: 0,
}));

const failStaleCrawlRuns = vi.fn(async () => 0);
const listCrawlRuns = vi.fn(async () => []);

const getAgentOpsSummary = vi.fn(async () => ({}));
const listChapterEvidence = vi.fn(async () => []);
const listChapterSearchRuns = vi.fn(async () => []);
const listOpsAlertsForAgentOps = vi.fn(async () => []);
const listProvisionalChapters = vi.fn(async () => []);
const listRequestGraphRuns = vi.fn(async () => []);
const listFieldJobs = vi.fn(async () => []);
const getFieldJobLogFeed = vi.fn(async () => ({
  jobId: "job-1",
  lines: [],
  dedupedCount: 0,
  generatedAt: new Date().toISOString(),
}));

const listCampaignRuns = vi.fn(async () => [
  {
    id: "campaign-1",
    status: "running",
    telemetry: {
      providerHealth: null,
      providerHealthHistory: [],
      activeConcurrency: 1,
      lastCheckpointAt: null,
      lastTuneAt: null,
      runtimeNotes: [],
      cohortManifest: [],
      activePolicyVersion: null,
      activePolicySnapshotId: null,
      promotionDecisions: [],
      queueStallAlert: null,
      delayedRewardHealth: null,
      reviewReasonDrift: [],
      acceptanceGate: null,
      baselineSnapshot: null,
      finalSnapshot: null,
      programPhase: "standard",
      programStartedAt: null,
    },
    items: [],
    events: [],
  },
]);
const getCampaignRunCounts = vi.fn(async () => ({
  total: 1,
  queued: 0,
  running: 1,
  succeeded: 0,
  failed: 0,
}));
const createCampaignRun = vi.fn(async () => ({
  id: "campaign-created",
  status: "queued",
  scheduledFor: new Date().toISOString(),
  runtimeWorkerId: null,
  runtimeLeaseExpiresAt: null,
  config: {
    targetCount: 20,
    controlCount: 2,
    activeConcurrency: 2,
    maxDurationMinutes: 60,
    checkpointIntervalMs: 60_000,
    tuningIntervalMs: 60_000,
    itemPollIntervalMs: 15_000,
    preflightRequired: false,
    autoTuningEnabled: false,
    controlFraternitySlugs: [],
    programMode: "standard",
    runtimeMode: "adaptive_primary",
    fieldJobRuntimeMode: "langgraph_primary",
    frozenSourceSlugs: [],
    trainingRounds: 1,
    epochsPerRound: 1,
    trainingSourceBatchSize: 1,
    evalSourceBatchSize: 1,
    trainingCommandTimeoutMinutes: 30,
    checkpointPromotionEnabled: false,
    queueStallThresholdMinutes: 15,
    reviewWindowDays: 14,
  },
  telemetry: {
    providerHealth: null,
    providerHealthHistory: [],
    activeConcurrency: 1,
    lastCheckpointAt: null,
    lastTuneAt: null,
    runtimeNotes: [],
    cohortManifest: [],
    activePolicyVersion: null,
    activePolicySnapshotId: null,
    promotionDecisions: [],
    queueStallAlert: null,
    delayedRewardHealth: null,
    reviewReasonDrift: [],
    acceptanceGate: null,
    baselineSnapshot: null,
    finalSnapshot: null,
    programPhase: "standard",
    programStartedAt: null,
  },
  items: [],
  events: [],
}));
const getCampaignRun = vi.fn(async () => ({
  id: "campaign-1",
  status: "queued",
  telemetry: {
    providerHealth: null,
    providerHealthHistory: [],
    activeConcurrency: 1,
    lastCheckpointAt: null,
    lastTuneAt: null,
    runtimeNotes: [],
    cohortManifest: [],
    activePolicyVersion: null,
    activePolicySnapshotId: null,
    promotionDecisions: [],
    queueStallAlert: null,
    delayedRewardHealth: null,
    reviewReasonDrift: [],
    acceptanceGate: null,
    baselineSnapshot: null,
    finalSnapshot: null,
    programPhase: "standard",
    programStartedAt: null,
  },
  items: [],
  events: [],
}));

const activeCampaignRunCount = vi.fn(() => 0);
const scheduleBenchmarkDriftAlertScan = vi.fn(async () => null);
const createEvaluationJob = vi.fn(async () => ({ id: "evaluation-job-1" }));
const scheduleDueFraternityCrawlRequests = vi.fn(async () => 0);
const scheduleFraternityCrawlRequest = vi.fn(async () => undefined);
const reconcileStaleFraternityCrawlRequests = vi.fn(async () => 0);
const listFraternityCrawlRequests = vi.fn(async () => []);
const getFraternityCrawlRequest = vi.fn(async () => null);
const getFraternityCrawlRequestCounts = vi.fn(async () => ({
  total: 1,
  draft: 0,
  queued: 0,
  running: 0,
  succeeded: 1,
  failed: 0,
  canceled: 0,
}));

vi.mock("@/lib/repositories/campaign-run-repository", () => ({
  listCampaignRuns,
  getCampaignRunCounts,
  createCampaignRun,
  reconcileStaleCampaignRuns,
  getCampaignRun,
}));

vi.mock("@/lib/campaign-runner", () => ({
  scheduleDueCampaignRuns,
  scheduleCampaignRun,
  isCampaignRunActive,
  activeCampaignRunCount,
}));

vi.mock("@/lib/repositories/evaluation-job-repository", () => ({
  createEvaluationJob,
  getEvaluationJobByRun: vi.fn(async () => null),
  cancelEvaluationJob: vi.fn(async () => undefined),
}));

vi.mock("@/lib/repositories/runtime-worker-repository", () => ({
  countActiveRuntimeWorkersByLane,
}));

vi.mock("@/lib/repositories/benchmark-repository", () => ({
  createBenchmarkRun,
  failStaleBenchmarkRuns,
  getBenchmarkRun,
  listBenchmarkRuns,
  getBenchmarkRunCounts,
}));

vi.mock("@/lib/repositories/crawl-run-repository", () => ({
  failStaleCrawlRuns,
  listCrawlRuns,
}));

vi.mock("@/lib/repositories/agent-ops-repository", () => ({
  getAgentOpsSummary,
  listChapterEvidence,
  listChapterSearchRuns,
  listOpsAlertsForAgentOps,
  listProvisionalChapters,
  listRequestGraphRuns,
}));

vi.mock("@/lib/repositories/field-job-repository", () => ({
  listFieldJobs,
  getFieldJobLogFeed,
}));

vi.mock("@/lib/benchmark-alerts", () => ({
  scheduleBenchmarkDriftAlertScan,
}));

vi.mock("@/lib/fraternity-crawl-request-runner", () => ({
  scheduleDueFraternityCrawlRequests,
  scheduleFraternityCrawlRequest,
}));

vi.mock("@/lib/repositories/fraternity-crawl-request-repository", () => ({
  listFraternityCrawlRequests,
  getFraternityCrawlRequestCounts,
  getFraternityCrawlRequest,
  reconcileStaleFraternityCrawlRequests,
  createFraternityCrawlRequest: vi.fn(),
  appendFraternityCrawlRequestEvent: vi.fn(),
  upsertFraternityRecord: vi.fn(),
  upsertSourceRecord: vi.fn(),
  updateFraternityCrawlRequest: vi.fn(),
  bumpQueuedFieldJobsForSource: vi.fn(),
}));

describe("read-only API routes", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("campaign-runs GET does not mutate runtime state", async () => {
    const route = await import("./campaign-runs/route");
    const response = await route.GET({
      nextUrl: new URL("http://localhost/api/campaign-runs?limit=5"),
    } as never);
    expect(response.status).toBe(200);
    expect(reconcileStaleCampaignRuns).not.toHaveBeenCalled();
    expect(scheduleDueCampaignRuns).not.toHaveBeenCalled();
    expect(scheduleCampaignRun).not.toHaveBeenCalled();
  });

  it("campaign detail GET does not auto-schedule or reconcile", async () => {
    const route = await import("./campaign-runs/[id]/route");
    const response = await route.GET(new Request("http://localhost/api/campaign-runs/campaign-1"), {
      params: { id: "campaign-1" },
    });
    expect(response.status).toBe(200);
    expect(reconcileStaleCampaignRuns).not.toHaveBeenCalled();
    expect(scheduleCampaignRun).not.toHaveBeenCalled();
  });

  it("campaign summary GET is observational only", async () => {
    const route = await import("./campaign-runs/summary/route");
    const response = await route.GET();
    expect(response.status).toBe(200);
    expect(getCampaignRunCounts).toHaveBeenCalled();
    expect(reconcileStaleCampaignRuns).not.toHaveBeenCalled();
    expect(scheduleCampaignRun).not.toHaveBeenCalled();
  });

  it("benchmarks GET does not fail stale runs", async () => {
    const route = await import("./benchmarks/route");
    const response = await route.GET({
      nextUrl: new URL("http://localhost/api/benchmarks?limit=10"),
    } as never);
    expect(response.status).toBe(200);
    expect(failStaleBenchmarkRuns).not.toHaveBeenCalled();
  });

  it("benchmark summary GET is observational only", async () => {
    const route = await import("./benchmarks/summary/route");
    const response = await route.GET();
    expect(response.status).toBe(200);
    expect(getBenchmarkRunCounts).toHaveBeenCalled();
    expect(failStaleBenchmarkRuns).not.toHaveBeenCalled();
  });

  it("benchmark detail GET does not fail stale runs", async () => {
    const route = await import("./benchmarks/[id]/route");
    const response = await route.GET(new Request("http://localhost/api/benchmarks/benchmark-1"), {
      params: { id: "benchmark-1" },
    });
    expect(response.status).toBe(404);
    expect(failStaleBenchmarkRuns).not.toHaveBeenCalled();
  });

  it("benchmark export GET does not fail stale runs", async () => {
    const route = await import("./benchmarks/[id]/export/route");
    const response = await route.GET(new Request("http://localhost/api/benchmarks/benchmark-1/export?format=json"), {
      params: { id: "benchmark-1" },
    });
    expect(response.status).toBe(404);
    expect(failStaleBenchmarkRuns).not.toHaveBeenCalled();
  });

  it("runs GET does not reconcile stale crawl runs", async () => {
    const route = await import("./runs/route");
    const response = await route.GET({
      nextUrl: new URL("http://localhost/api/runs?limit=10"),
    } as never);
    expect(response.status).toBe(200);
    expect(failStaleCrawlRuns).not.toHaveBeenCalled();
  });

  it("agent-ops GET does not reconcile stale crawl runs", async () => {
    const route = await import("./agent-ops/route");
    const response = await route.GET({
      nextUrl: new URL("http://localhost/api/agent-ops?limit=10"),
    } as never);
    expect(response.status).toBe(200);
    expect(failStaleCrawlRuns).not.toHaveBeenCalled();
  });

  it("field-jobs GET is observational only", async () => {
    const route = await import("./field-jobs/route");
    const response = await route.GET({
      nextUrl: new URL("http://localhost/api/field-jobs?limit=10"),
    } as never);
    expect(response.status).toBe(200);
    expect(listFieldJobs).toHaveBeenCalled();
    expect(scheduleDueCampaignRuns).not.toHaveBeenCalled();
  });

  it("field-job logs GET is observational only", async () => {
    const route = await import("./field-jobs/[id]/logs/route");
    const response = await route.GET(new Request("http://localhost/api/field-jobs/job-1/logs?limit=20"), {
      params: { id: "job-1" },
    });
    expect(response.status).toBe(200);
    expect(getFieldJobLogFeed).toHaveBeenCalledWith("job-1", 20);
    expect(scheduleDueCampaignRuns).not.toHaveBeenCalled();
  });

  it("health GET is observational only", async () => {
    const route = await import("./health/route");
    const response = await route.GET();
    expect(response.status).toBe(200);
    expect(scheduleDueCampaignRuns).not.toHaveBeenCalled();
    expect(reconcileStaleCampaignRuns).not.toHaveBeenCalled();
    expect(scheduleBenchmarkDriftAlertScan).not.toHaveBeenCalled();
  });

  it("fraternity crawl requests GET does not reconcile or schedule", async () => {
    const route = await import("./fraternity-crawl-requests/route");
    const response = await route.GET({
      nextUrl: new URL("http://localhost/api/fraternity-crawl-requests?limit=10"),
    } as never);
    expect(response.status).toBe(200);
    expect(reconcileStaleFraternityCrawlRequests).not.toHaveBeenCalled();
    expect(scheduleDueFraternityCrawlRequests).not.toHaveBeenCalled();
  });

  it("fraternity crawl request detail GET does not reconcile or schedule", async () => {
    const route = await import("./fraternity-crawl-requests/[id]/route");
    const response = await route.GET(new Request("http://localhost/api/fraternity-crawl-requests/request-1") as never, {
      params: { id: "request-1" },
    });
    expect(response.status).toBe(404);
    expect(reconcileStaleFraternityCrawlRequests).not.toHaveBeenCalled();
    expect(scheduleDueFraternityCrawlRequests).not.toHaveBeenCalled();
    expect(scheduleFraternityCrawlRequest).not.toHaveBeenCalled();
  });

  it("fraternity crawl request summary GET is observational only", async () => {
    const route = await import("./fraternity-crawl-requests/summary/route");
    const response = await route.GET();
    expect(response.status).toBe(200);
    expect(getFraternityCrawlRequestCounts).toHaveBeenCalled();
    expect(reconcileStaleFraternityCrawlRequests).not.toHaveBeenCalled();
    expect(scheduleDueFraternityCrawlRequests).not.toHaveBeenCalled();
  });

  it("benchmarks POST enqueues evaluation work instead of scheduling in-process", async () => {
    const route = await import("./benchmarks/route");
    const response = await route.POST(
      new Request("http://localhost/api/benchmarks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fieldName: "find_email",
          workers: 1,
          limitPerCycle: 1,
          cycles: 1,
          pauseMs: 0,
        }),
      }) as never
    );
    expect(response.status).toBe(202);
    expect(createBenchmarkRun).toHaveBeenCalled();
    expect(createEvaluationJob).toHaveBeenCalled();
  });

  it("campaign-runs POST enqueues evaluation work instead of scheduling in-process", async () => {
    const route = await import("./campaign-runs/route");
    const response = await route.POST(
      new Request("http://localhost/api/campaign-runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: "campaign" }),
      }) as never
    );
    expect(response.status).toBe(202);
    expect(createCampaignRun).toHaveBeenCalled();
    expect(createEvaluationJob).toHaveBeenCalled();
  });
});
