import { apiSuccess, toApiErrorResponse } from "@/lib/api-envelope";
import { getDbPool } from "@/lib/db";
import { countActiveRuntimeWorkersByLane } from "@/lib/repositories/runtime-worker-repository";

export const dynamic = "force-dynamic";

async function getCrawlerSnapshot() {
  const dbPool = getDbPool();
  const [requestResult, queueResult] = await Promise.all([
    dbPool.query<{
      running_requests: number;
      queued_requests: number;
      failed_requests: number;
      draft_requests: number;
    }>(
      `
        SELECT
          COUNT(*) FILTER (WHERE status = 'running')::int AS running_requests,
          COUNT(*) FILTER (WHERE status = 'queued')::int AS queued_requests,
          COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_requests,
          COUNT(*) FILTER (WHERE status IN ('draft', 'awaiting_confirmation'))::int AS draft_requests
        FROM fraternity_crawl_requests
      `
    ),
    dbPool.query<{
      queued_jobs: number;
      actionable_jobs: number;
      deferred_jobs: number;
      blocked_provider_jobs: number;
      blocked_dependency_jobs: number;
      blocked_repairable_jobs: number;
      running_jobs: number;
    }>(
      `
        SELECT
          COUNT(*) FILTER (WHERE status = 'queued')::int AS queued_jobs,
          COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'actionable')::int AS actionable_jobs,
          COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'deferred')::int AS deferred_jobs,
          COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_provider')::int AS blocked_provider_jobs,
          COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_dependency')::int AS blocked_dependency_jobs,
          COUNT(*) FILTER (WHERE status = 'queued' AND COALESCE(queue_state, 'actionable') = 'blocked_repairable')::int AS blocked_repairable_jobs,
          COUNT(*) FILTER (WHERE status = 'running')::int AS running_jobs
        FROM field_jobs
        WHERE field_name IN ('find_website', 'verify_website', 'find_instagram', 'find_email', 'verify_school_match')
      `
    ),
  ]);

  return {
    requests: {
      running: Number(requestResult.rows[0]?.running_requests ?? 0),
      queued: Number(requestResult.rows[0]?.queued_requests ?? 0),
      failed: Number(requestResult.rows[0]?.failed_requests ?? 0),
      draft: Number(requestResult.rows[0]?.draft_requests ?? 0),
    },
    queue: {
      queuedJobs: Number(queueResult.rows[0]?.queued_jobs ?? 0),
      actionableJobs: Number(queueResult.rows[0]?.actionable_jobs ?? 0),
      deferredJobs: Number(queueResult.rows[0]?.deferred_jobs ?? 0),
      blockedProviderJobs: Number(queueResult.rows[0]?.blocked_provider_jobs ?? 0),
      blockedDependencyJobs: Number(queueResult.rows[0]?.blocked_dependency_jobs ?? 0),
      blockedRepairableJobs: Number(queueResult.rows[0]?.blocked_repairable_jobs ?? 0),
      runningJobs: Number(queueResult.rows[0]?.running_jobs ?? 0),
    },
  };
}

async function probeSearxng() {
  const baseUrl = String(process.env.CRAWLER_SEARCH_SEARXNG_BASE_URL ?? "").trim();
  if (!baseUrl) {
    return {
      configured: false,
      reachable: false,
      statusCode: null as number | null,
      latencyMs: null as number | null,
      reason: "not_configured",
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5_000);
  const startedAt = Date.now();
  try {
    const response = await fetch(`${baseUrl.replace(/\/$/, "")}/search?q=health&format=json`, {
      method: "GET",
      signal: controller.signal,
      cache: "no-store",
      headers: {
        Accept: "application/json",
      },
    });
    return {
      configured: true,
      reachable: response.ok,
      statusCode: response.status,
      latencyMs: Date.now() - startedAt,
      reason: response.ok ? "reachable" : `http_${response.status}`,
    };
  } catch (error) {
    return {
      configured: true,
      reachable: false,
      statusCode: null,
      latencyMs: Date.now() - startedAt,
      reason: error instanceof Error ? error.name.toLowerCase() : "request_failed",
    };
  } finally {
    clearTimeout(timeout);
  }
}

export async function GET() {
  try {
    const [activeWorkers, crawler, searxng] = await Promise.all([
      countActiveRuntimeWorkersByLane(["campaign", "benchmark", "evaluation", "request", "contact_resolution", "chapter_repair"]),
      getCrawlerSnapshot(),
      probeSearxng(),
    ]);

    const activeRequestWorkers = Number(activeWorkers.request ?? 0);
    const activeFieldJobWorkers = Number(activeWorkers.contact_resolution ?? 0);
    const queueWorkerAlert = crawler.queue.actionableJobs > 0 && crawler.queue.runningJobs === 0 && activeFieldJobWorkers === 0;
    const requestWorkerAlert = crawler.requests.running > 0 && activeRequestWorkers === 0;
    const ok = !queueWorkerAlert && !requestWorkerAlert && (!searxng.configured || searxng.reachable);

    return apiSuccess({
      ok,
      runtime: {
        activeCampaignRuns: Number(activeWorkers.campaign ?? 0),
        activeBenchmarkRuns: Number(activeWorkers.benchmark ?? 0),
        activeEvaluationWorkers: Number(activeWorkers.evaluation ?? 0),
        activeRequestWorkers,
        activeFieldJobWorkers,
        activeChapterRepairWorkers: Number(activeWorkers.chapter_repair ?? 0),
        mutatingReadPathsDisabled: true,
      },
      crawler,
      search: {
        searxng,
      },
      alerts: {
        queueWorkerAlert,
        requestWorkerAlert,
      },
      checkedAt: new Date().toISOString()
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
