from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


_REPO_MARKERS = ("pnpm-workspace.yaml", ".git")


def resolve_repo_root(start: Path | None = None) -> Path:
    current = (start or Path(__file__)).resolve()
    for candidate in (current, *current.parents):
        if any((candidate / marker).exists() for marker in _REPO_MARKERS):
            return candidate
    return Path(__file__).resolve().parents[4]


def resolve_env_file_path(explicit_path: str | None = None) -> Path | None:
    candidate = (explicit_path or os.getenv("CRAWLER_ENV_FILE") or os.getenv("FRATFINDER_ENV_FILE") or "").strip()
    if candidate:
        path = Path(candidate).expanduser()
        if not path.is_absolute():
            path = resolve_repo_root() / path
        return path.resolve()

    repo_env = resolve_repo_root() / ".env"
    if repo_env.exists():
        return repo_env

    cwd_env = Path.cwd() / ".env"
    if cwd_env.exists():
        return cwd_env.resolve()

    return None


def resolved_env_file_path() -> str | None:
    path = resolve_env_file_path()
    return str(path) if path is not None else None


def settings_deprecation_warnings() -> list[str]:
    warnings: list[str] = []
    if any(key.startswith("Agent:") for key in os.environ):
        warnings.append("Legacy `Agent:*` env keys are still present; migrate to `CRAWLER_*` keys.")
    return warnings


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=resolved_env_file_path(),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    app_env: str = Field(default="development", alias="APP_ENV")
    database_url: str = Field(alias="DATABASE_URL")

    crawler_log_level: str = Field(default="INFO", alias="CRAWLER_LOG_LEVEL")
    crawler_http_timeout_seconds: float = Field(default=20.0, alias="CRAWLER_HTTP_TIMEOUT_SECONDS")
    crawler_http_verify_ssl: bool = Field(default=True, alias="CRAWLER_HTTP_VERIFY_SSL")
    crawler_http_user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        alias="CRAWLER_HTTP_USER_AGENT"
    )
    crawler_max_retries: int = Field(default=3, alias="CRAWLER_MAX_RETRIES")
    crawler_retry_backoff_seconds: float = Field(default=1.0, alias="CRAWLER_RETRY_BACKOFF_SECONDS")
    crawler_field_job_worker_id: str = Field(default="local-crawler-worker", alias="CRAWLER_FIELD_JOB_WORKER_ID")
    crawler_field_job_base_backoff_seconds: int = Field(default=30, alias="CRAWLER_FIELD_JOB_BASE_BACKOFF_SECONDS")
    crawler_field_job_max_workers: int = Field(default=12, alias="CRAWLER_FIELD_JOB_MAX_WORKERS")
    crawler_field_job_stale_claim_minutes: int = Field(default=60, alias="CRAWLER_FIELD_JOB_STALE_CLAIM_MINUTES")
    crawler_field_job_graph_run_stale_minutes: int = Field(default=60, alias="CRAWLER_FIELD_JOB_GRAPH_RUN_STALE_MINUTES")
    crawler_field_job_worker_lease_seconds: int = Field(default=180, alias="CRAWLER_FIELD_JOB_WORKER_LEASE_SECONDS")
    crawler_field_job_worker_heartbeat_seconds: int = Field(default=30, alias="CRAWLER_FIELD_JOB_WORKER_HEARTBEAT_SECONDS")
    crawler_field_job_liveness_alert_poll_windows: int = Field(default=2, alias="CRAWLER_FIELD_JOB_LIVENESS_ALERT_POLL_WINDOWS")
    crawler_field_job_runtime_mode: str = Field(
        default="langgraph_primary",
        alias="Agent:FIELD_JOB_RUNTIME_MODE",
        validation_alias=AliasChoices("CRAWLER_FIELD_JOB_RUNTIME_MODE", "Agent:FIELD_JOB_RUNTIME_MODE"),
    )
    crawler_field_job_graph_durability: str = Field(
        default="sync",
        alias="Agent:FIELD_JOB_GRAPH_DURABILITY",
        validation_alias=AliasChoices("CRAWLER_FIELD_JOB_GRAPH_DURABILITY", "Agent:FIELD_JOB_GRAPH_DURABILITY"),
    )
    crawler_search_enabled: bool = Field(default=True, alias="CRAWLER_SEARCH_ENABLED")
    crawler_search_provider: str = Field(default="auto", alias="CRAWLER_SEARCH_PROVIDER")
    crawler_search_provider_order_free: str = Field(
        default="searxng_json,duckduckgo_html,bing_html",
        alias="CRAWLER_SEARCH_PROVIDER_ORDER_FREE",
    )
    crawler_search_max_results: int = Field(default=5, alias="CRAWLER_SEARCH_MAX_RESULTS")
    crawler_search_max_pages_per_job: int = Field(default=3, alias="CRAWLER_SEARCH_MAX_PAGES_PER_JOB")
    crawler_search_cache_empty_results: bool = Field(default=False, alias="CRAWLER_SEARCH_CACHE_EMPTY_RESULTS")
    crawler_search_circuit_breaker_failures: int = Field(default=3, alias="CRAWLER_SEARCH_CIRCUIT_BREAKER_FAILURES")
    crawler_search_circuit_breaker_cooldown_seconds: int = Field(default=60, alias="CRAWLER_SEARCH_CIRCUIT_BREAKER_COOLDOWN_SECONDS")
    crawler_search_min_request_interval_ms: int = Field(default=0, alias="CRAWLER_SEARCH_MIN_REQUEST_INTERVAL_MS")
    crawler_search_provider_pacing_ms_searxng_json: int = Field(default=0, alias="CRAWLER_SEARCH_PROVIDER_PACING_MS_SEARXNG_JSON")
    crawler_search_provider_pacing_ms_tavily_api: int = Field(default=0, alias="CRAWLER_SEARCH_PROVIDER_PACING_MS_TAVILY_API")
    crawler_search_provider_pacing_ms_serper_api: int = Field(default=0, alias="CRAWLER_SEARCH_PROVIDER_PACING_MS_SERPER_API")
    crawler_search_provider_pacing_ms_bing_html: int = Field(default=0, alias="CRAWLER_SEARCH_PROVIDER_PACING_MS_BING_HTML")
    crawler_search_provider_pacing_ms_duckduckgo_html: int = Field(default=0, alias="CRAWLER_SEARCH_PROVIDER_PACING_MS_DUCKDUCKGO_HTML")
    crawler_search_provider_pacing_ms_brave_html: int = Field(default=0, alias="CRAWLER_SEARCH_PROVIDER_PACING_MS_BRAVE_HTML")
    crawler_search_negative_cooldown_days: int = Field(default=1, alias="CRAWLER_SEARCH_NEGATIVE_COOLDOWN_DAYS")
    crawler_search_dependency_wait_seconds: int = Field(default=180, alias="CRAWLER_SEARCH_DEPENDENCY_WAIT_SECONDS")
    crawler_search_transient_short_retries: int = Field(default=2, alias="CRAWLER_SEARCH_TRANSIENT_SHORT_RETRIES")
    crawler_search_transient_long_cooldown_seconds: int = Field(default=900, alias="CRAWLER_SEARCH_TRANSIENT_LONG_COOLDOWN_SECONDS")
    crawler_search_require_confident_website_for_email: bool = Field(default=True, alias="CRAWLER_SEARCH_REQUIRE_CONFIDENT_WEBSITE_FOR_EMAIL")
    crawler_search_email_escape_on_provider_block: bool = Field(default=True, alias="CRAWLER_SEARCH_EMAIL_ESCAPE_ON_PROVIDER_BLOCK")
    crawler_search_email_escape_min_website_failures: int = Field(default=2, alias="CRAWLER_SEARCH_EMAIL_ESCAPE_MIN_WEBSITE_FAILURES")
    crawler_search_min_no_candidate_backoff_seconds: int = Field(default=60, alias="CRAWLER_SEARCH_MIN_NO_CANDIDATE_BACKOFF_SECONDS")
    crawler_search_email_max_queries: int = Field(default=4, alias="CRAWLER_SEARCH_EMAIL_MAX_QUERIES")
    crawler_search_instagram_max_queries: int = Field(default=5, alias="CRAWLER_SEARCH_INSTAGRAM_MAX_QUERIES")
    crawler_search_preflight_enabled: bool = Field(default=False, alias="CRAWLER_SEARCH_PREFLIGHT_ENABLED")
    crawler_search_preflight_probe_count: int = Field(default=3, alias="CRAWLER_SEARCH_PREFLIGHT_PROBE_COUNT")
    crawler_search_preflight_min_success_rate: float = Field(default=0.34, alias="CRAWLER_SEARCH_PREFLIGHT_MIN_SUCCESS_RATE")
    crawler_search_mid_batch_recheck_enabled: bool = Field(default=True, alias="CRAWLER_SEARCH_MID_BATCH_RECHECK_ENABLED")
    crawler_search_mid_batch_recheck_every_jobs: int = Field(default=40, alias="CRAWLER_SEARCH_MID_BATCH_RECHECK_EVERY_JOBS")
    crawler_search_mid_batch_recheck_every_seconds: int = Field(default=120, alias="CRAWLER_SEARCH_MID_BATCH_RECHECK_EVERY_SECONDS")
    crawler_search_mid_batch_min_success_rate: float = Field(default=0.25, alias="CRAWLER_SEARCH_MID_BATCH_MIN_SUCCESS_RATE")
    crawler_search_degraded_worker_cap: int = Field(default=4, alias="CRAWLER_SEARCH_DEGRADED_WORKER_CAP")
    crawler_search_degraded_max_results: int = Field(default=2, alias="CRAWLER_SEARCH_DEGRADED_MAX_RESULTS")
    crawler_search_degraded_max_pages_per_job: int = Field(default=1, alias="CRAWLER_SEARCH_DEGRADED_MAX_PAGES_PER_JOB")
    crawler_search_degraded_email_max_queries: int = Field(default=2, alias="CRAWLER_SEARCH_DEGRADED_EMAIL_MAX_QUERIES")
    crawler_search_degraded_instagram_max_queries: int = Field(default=3, alias="CRAWLER_SEARCH_DEGRADED_INSTAGRAM_MAX_QUERIES")
    crawler_search_degraded_dependency_wait_seconds: int = Field(default=600, alias="CRAWLER_SEARCH_DEGRADED_DEPENDENCY_WAIT_SECONDS")
    crawler_search_enable_school_initials: bool = Field(default=True, alias="CRAWLER_SEARCH_ENABLE_SCHOOL_INITIALS")
    crawler_search_min_school_initial_length: int = Field(default=3, alias="CRAWLER_SEARCH_MIN_SCHOOL_INITIAL_LENGTH")
    crawler_search_enable_compact_fraternity: bool = Field(default=True, alias="CRAWLER_SEARCH_ENABLE_COMPACT_FRATERNITY")
    crawler_search_instagram_enable_handle_queries: bool = Field(default=True, alias="CRAWLER_SEARCH_INSTAGRAM_ENABLE_HANDLE_QUERIES")
    crawler_search_instagram_direct_probe_enabled: bool = Field(default=False, alias="CRAWLER_SEARCH_INSTAGRAM_DIRECT_PROBE_ENABLED")
    crawler_search_searxng_base_url: str | None = Field(default=None, alias="CRAWLER_SEARCH_SEARXNG_BASE_URL")
    crawler_search_searxng_engines: str | None = Field(default=None, alias="CRAWLER_SEARCH_SEARXNG_ENGINES")
    crawler_search_tavily_api_key: str | None = Field(default=None, alias="CRAWLER_SEARCH_TAVILY_API_KEY")
    crawler_search_serper_api_key: str | None = Field(default=None, alias="CRAWLER_SEARCH_SERPER_API_KEY")
    crawler_discovery_verified_min_confidence: float = Field(default=0.65, alias="CRAWLER_DISCOVERY_VERIFIED_MIN_CONFIDENCE")
    crawler_navigation_max_hops_per_stub: int = Field(default=2, alias="CRAWLER_NAV_MAX_HOPS_PER_STUB")
    crawler_navigation_max_pages_per_run: int = Field(default=60, alias="CRAWLER_NAV_MAX_PAGES_PER_RUN")
    crawler_greedy_collect: str = Field(default="none", alias="GREEDY_COLLECT")
    crawler_search_brave_api_key: str | None = Field(default=None, alias="CRAWLER_SEARCH_BRAVE_API_KEY")
    crawler_llm_enabled: bool = Field(default=False, alias="CRAWLER_LLM_ENABLED")
    crawler_llm_model: str = Field(default="gpt-4o-mini", alias="CRAWLER_LLM_MODEL")
    crawler_llm_max_tokens: int = Field(default=2000, alias="CRAWLER_LLM_MAX_TOKENS")
    crawler_llm_max_calls_per_run: int = Field(default=3, alias="CRAWLER_LLM_MAX_CALLS_PER_RUN")
    crawler_runtime_mode: str = Field(default="adaptive_assisted", alias="CRAWLER_RUNTIME_MODE")
    crawler_adaptive_enabled: bool = Field(default=False, alias="CRAWLER_ADAPTIVE_ENABLED")
    crawler_v3_enabled: bool = Field(default=False, alias="CRAWLER_V3_ENABLED")
    crawler_v3_execution_mode: str = Field(default="worker_service", alias="CRAWLER_V3_EXECUTION_MODE")
    crawler_v3_request_worker_id: str = Field(default="local-request-worker", alias="CRAWLER_V3_REQUEST_WORKER_ID")
    crawler_v3_request_worker_runtime_owner: str = Field(default="python_request_worker", alias="CRAWLER_V3_REQUEST_WORKER_RUNTIME_OWNER")
    crawler_v3_request_poll_seconds: int = Field(default=2, alias="CRAWLER_V3_REQUEST_POLL_SECONDS")
    crawler_v3_request_batch_limit: int = Field(default=20, alias="CRAWLER_V3_REQUEST_BATCH_LIMIT")
    crawler_v3_request_stale_minutes: int = Field(default=45, alias="CRAWLER_V3_REQUEST_STALE_MINUTES")
    crawler_v3_request_worker_lease_seconds: int = Field(default=180, alias="CRAWLER_V3_REQUEST_WORKER_LEASE_SECONDS")
    crawler_v3_request_worker_heartbeat_seconds: int = Field(default=30, alias="CRAWLER_V3_REQUEST_WORKER_HEARTBEAT_SECONDS")
    crawler_v3_free_recovery_attempts: int = Field(default=3, alias="CRAWLER_V3_FREE_RECOVERY_ATTEMPTS")
    crawler_v3_crawl_runtime_mode: str = Field(default="adaptive_assisted", alias="CRAWLER_V3_CRAWL_RUNTIME_MODE")
    crawler_v3_field_job_runtime_mode: str = Field(default="langgraph_primary", alias="CRAWLER_V3_FIELD_JOB_RUNTIME_MODE")
    crawler_v3_field_job_graph_durability: str = Field(default="sync", alias="CRAWLER_V3_FIELD_JOB_GRAPH_DURABILITY")
    crawler_v3_paid_search_enabled: bool = Field(default=False, alias="CRAWLER_V3_PAID_SEARCH_ENABLED")
    crawler_v3_llm_enabled: bool = Field(default=False, alias="CRAWLER_V3_LLM_ENABLED")
    crawler_v3_provisional_promotion_mode: str = Field(default="single_strong_official", alias="CRAWLER_V3_PROVISIONAL_PROMOTION_MODE")
    crawler_v3_reward_weights: str = Field(default='{"precision":0.45,"coverage":0.35,"efficiency":0.2}', alias="CRAWLER_V3_REWARD_WEIGHTS")
    crawler_frontier_max_pages_per_source: int = Field(default=40, alias="CRAWLER_FRONTIER_MAX_PAGES_PER_SOURCE")
    crawler_frontier_max_depth: int = Field(default=3, alias="CRAWLER_FRONTIER_MAX_DEPTH")
    crawler_frontier_max_pages_per_template: int = Field(default=8, alias="CRAWLER_FRONTIER_MAX_PAGES_PER_TEMPLATE")
    crawler_frontier_max_empty_streak: int = Field(default=5, alias="CRAWLER_FRONTIER_MAX_EMPTY_STREAK")
    crawler_frontier_high_yield_record_threshold: int = Field(default=60, alias="CRAWLER_FRONTIER_HIGH_YIELD_RECORD_THRESHOLD")
    crawler_frontier_min_pages_for_high_yield_stop: int = Field(default=2, alias="CRAWLER_FRONTIER_MIN_PAGES_FOR_HIGH_YIELD_STOP")
    crawler_adaptive_epsilon: float = Field(default=0.1, alias="CRAWLER_ADAPTIVE_EPSILON")
    crawler_adaptive_min_score: float = Field(default=0.1, alias="CRAWLER_ADAPTIVE_MIN_SCORE")
    crawler_adaptive_stop_saturation_threshold: int = Field(default=4, alias="CRAWLER_ADAPTIVE_STOP_SATURATION_THRESHOLD")
    crawler_policy_version: str = Field(default="adaptive-v1", alias="CRAWLER_POLICY_VERSION")
    crawler_replay_export_limit: int = Field(default=500, alias="CRAWLER_REPLAY_EXPORT_LIMIT")

    crawler_adaptive_policy_restore_enabled: bool = Field(
        default=True,
        alias="Agent:ADAPTIVE_POLICY_RESTORE_ENABLED",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_POLICY_RESTORE_ENABLED", "Agent:ADAPTIVE_POLICY_RESTORE_ENABLED"),
    )
    crawler_adaptive_train_default_epochs: int = Field(
        default=3,
        alias="Agent:ADAPTIVE_TRAIN_DEFAULT_EPOCHS",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_TRAIN_DEFAULT_EPOCHS", "Agent:ADAPTIVE_TRAIN_DEFAULT_EPOCHS"),
    )
    crawler_adaptive_train_default_runtime_mode: str = Field(
        default="adaptive_assisted",
        alias="Agent:ADAPTIVE_TRAIN_DEFAULT_RUNTIME_MODE",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_TRAIN_DEFAULT_RUNTIME_MODE", "Agent:ADAPTIVE_TRAIN_DEFAULT_RUNTIME_MODE"),
    )
    crawler_adaptive_train_source_slugs: str = Field(
        default="",
        alias="Agent:ADAPTIVE_TRAIN_SOURCE_SLUGS",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_TRAIN_SOURCE_SLUGS", "Agent:ADAPTIVE_TRAIN_SOURCE_SLUGS"),
    )
    crawler_adaptive_eval_source_slugs: str = Field(
        default="",
        alias="Agent:ADAPTIVE_EVAL_SOURCE_SLUGS",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_EVAL_SOURCE_SLUGS", "Agent:ADAPTIVE_EVAL_SOURCE_SLUGS"),
    )

    crawler_adaptive_eval_enrichment_limit_per_source: int = Field(
        default=120,
        alias="Agent:ADAPTIVE_EVAL_ENRICHMENT_LIMIT_PER_SOURCE",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_EVAL_ENRICHMENT_LIMIT_PER_SOURCE", "Agent:ADAPTIVE_EVAL_ENRICHMENT_LIMIT_PER_SOURCE"),
    )
    crawler_adaptive_eval_enrichment_workers: int = Field(
        default=4,
        alias="Agent:ADAPTIVE_EVAL_ENRICHMENT_WORKERS",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_EVAL_ENRICHMENT_WORKERS", "Agent:ADAPTIVE_EVAL_ENRICHMENT_WORKERS"),
    )
    crawler_adaptive_eval_enrichment_run_preflight: bool = Field(
        default=True,
        alias="Agent:ADAPTIVE_EVAL_ENRICHMENT_RUN_PREFLIGHT",
        validation_alias=AliasChoices(
            "CRAWLER_ADAPTIVE_EVAL_ENRICHMENT_RUN_PREFLIGHT",
            "Agent:ADAPTIVE_EVAL_ENRICHMENT_RUN_PREFLIGHT",
        ),
    )
    crawler_adaptive_eval_enrichment_require_healthy_search: bool = Field(
        default=True,
        alias="Agent:ADAPTIVE_EVAL_ENRICHMENT_REQUIRE_HEALTHY_SEARCH",
        validation_alias=AliasChoices(
            "CRAWLER_ADAPTIVE_EVAL_ENRICHMENT_REQUIRE_HEALTHY_SEARCH",
            "Agent:ADAPTIVE_EVAL_ENRICHMENT_REQUIRE_HEALTHY_SEARCH",
        ),
    )
    crawler_adaptive_live_epsilon: float = Field(
        default=0.02,
        alias="Agent:ADAPTIVE_LIVE_EPSILON",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_LIVE_EPSILON", "CRAWLER_ADAPTIVE_EPSILON", "Agent:ADAPTIVE_LIVE_EPSILON"),
    )
    crawler_adaptive_train_epsilon: float = Field(
        default=0.12,
        alias="Agent:ADAPTIVE_TRAIN_EPSILON",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_TRAIN_EPSILON", "Agent:ADAPTIVE_TRAIN_EPSILON"),
    )
    crawler_adaptive_reward_gamma: float = Field(
        default=0.85,
        alias="Agent:ADAPTIVE_REWARD_GAMMA",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_REWARD_GAMMA", "Agent:ADAPTIVE_REWARD_GAMMA"),
    )
    crawler_adaptive_trace_hops: int = Field(
        default=4,
        alias="Agent:ADAPTIVE_TRACE_HOPS",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_TRACE_HOPS", "Agent:ADAPTIVE_TRACE_HOPS"),
    )
    crawler_adaptive_replay_window_days: int = Field(
        default=7,
        alias="Agent:ADAPTIVE_REPLAY_WINDOW_DAYS",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_REPLAY_WINDOW_DAYS", "Agent:ADAPTIVE_REPLAY_WINDOW_DAYS"),
    )
    crawler_adaptive_replay_batch_size: int = Field(
        default=500,
        alias="Agent:ADAPTIVE_REPLAY_BATCH_SIZE",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_REPLAY_BATCH_SIZE", "Agent:ADAPTIVE_REPLAY_BATCH_SIZE"),
    )
    crawler_adaptive_risk_timeout_weight: float = Field(
        default=0.75,
        alias="Agent:ADAPTIVE_RISK_TIMEOUT_WEIGHT",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_RISK_TIMEOUT_WEIGHT", "Agent:ADAPTIVE_RISK_TIMEOUT_WEIGHT"),
    )
    crawler_adaptive_risk_requeue_weight: float = Field(
        default=0.35,
        alias="Agent:ADAPTIVE_RISK_REQUEUE_WEIGHT",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_RISK_REQUEUE_WEIGHT", "Agent:ADAPTIVE_RISK_REQUEUE_WEIGHT"),
    )
    crawler_adaptive_balanced_kpi_weights: str = Field(
        default='{"coverage":0.45,"throughput":0.2,"queue":0.2,"reliability":0.15}',
        alias="Agent:ADAPTIVE_BALANCED_KPI_WEIGHTS",
        validation_alias=AliasChoices("CRAWLER_ADAPTIVE_BALANCED_KPI_WEIGHTS", "Agent:ADAPTIVE_BALANCED_KPI_WEIGHTS"),
    )
    crawler_adaptive_enrichment_observations_enabled: bool = Field(
        default=True,
        alias="Agent:ADAPTIVE_ENRICHMENT_OBSERVATIONS_ENABLED",
        validation_alias=AliasChoices(
            "CRAWLER_ADAPTIVE_ENRICHMENT_OBSERVATIONS_ENABLED",
            "Agent:ADAPTIVE_ENRICHMENT_OBSERVATIONS_ENABLED",
        ),
    )

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    env_file = resolve_env_file_path()
    if env_file is not None:
        return Settings(_env_file=env_file)
    return Settings()
