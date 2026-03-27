from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True)

    app_env: str = Field(default="development", alias="APP_ENV")
    database_url: str = Field(alias="DATABASE_URL")

    crawler_log_level: str = Field(default="INFO", alias="CRAWLER_LOG_LEVEL")
    crawler_http_timeout_seconds: float = Field(default=20.0, alias="CRAWLER_HTTP_TIMEOUT_SECONDS")
    crawler_http_verify_ssl: bool = Field(default=True, alias="CRAWLER_HTTP_VERIFY_SSL")
    crawler_http_user_agent: str = Field(
        default="FratFinderAI/1.0 (+https://example.com/fratfinder)",
        alias="CRAWLER_HTTP_USER_AGENT"
    )
    crawler_max_retries: int = Field(default=3, alias="CRAWLER_MAX_RETRIES")
    crawler_retry_backoff_seconds: float = Field(default=1.0, alias="CRAWLER_RETRY_BACKOFF_SECONDS")
    crawler_field_job_worker_id: str = Field(default="local-crawler-worker", alias="CRAWLER_FIELD_JOB_WORKER_ID")
    crawler_field_job_base_backoff_seconds: int = Field(default=30, alias="CRAWLER_FIELD_JOB_BASE_BACKOFF_SECONDS")
    crawler_field_job_max_workers: int = Field(default=8, alias="CRAWLER_FIELD_JOB_MAX_WORKERS")
    crawler_search_enabled: bool = Field(default=True, alias="CRAWLER_SEARCH_ENABLED")
    crawler_search_provider: str = Field(default="auto", alias="CRAWLER_SEARCH_PROVIDER")
    crawler_search_max_results: int = Field(default=5, alias="CRAWLER_SEARCH_MAX_RESULTS")
    crawler_search_max_pages_per_job: int = Field(default=3, alias="CRAWLER_SEARCH_MAX_PAGES_PER_JOB")
    crawler_search_cache_empty_results: bool = Field(default=False, alias="CRAWLER_SEARCH_CACHE_EMPTY_RESULTS")
    crawler_search_circuit_breaker_failures: int = Field(default=3, alias="CRAWLER_SEARCH_CIRCUIT_BREAKER_FAILURES")
    crawler_search_circuit_breaker_cooldown_seconds: int = Field(default=60, alias="CRAWLER_SEARCH_CIRCUIT_BREAKER_COOLDOWN_SECONDS")
    crawler_search_negative_cooldown_days: int = Field(default=30, alias="CRAWLER_SEARCH_NEGATIVE_COOLDOWN_DAYS")
    crawler_search_dependency_wait_seconds: int = Field(default=300, alias="CRAWLER_SEARCH_DEPENDENCY_WAIT_SECONDS")
    crawler_search_min_no_candidate_backoff_seconds: int = Field(default=60, alias="CRAWLER_SEARCH_MIN_NO_CANDIDATE_BACKOFF_SECONDS")
    crawler_search_email_max_queries: int = Field(default=5, alias="CRAWLER_SEARCH_EMAIL_MAX_QUERIES")
    crawler_search_instagram_max_queries: int = Field(default=6, alias="CRAWLER_SEARCH_INSTAGRAM_MAX_QUERIES")
    crawler_search_enable_school_initials: bool = Field(default=True, alias="CRAWLER_SEARCH_ENABLE_SCHOOL_INITIALS")
    crawler_search_min_school_initial_length: int = Field(default=3, alias="CRAWLER_SEARCH_MIN_SCHOOL_INITIAL_LENGTH")
    crawler_search_enable_compact_fraternity: bool = Field(default=True, alias="CRAWLER_SEARCH_ENABLE_COMPACT_FRATERNITY")
    crawler_search_instagram_enable_handle_queries: bool = Field(default=True, alias="CRAWLER_SEARCH_INSTAGRAM_ENABLE_HANDLE_QUERIES")
    crawler_greedy_collect: str = Field(default="none", alias="GREEDY_COLLECT")
    crawler_search_brave_api_key: str | None = Field(default=None, alias="CRAWLER_SEARCH_BRAVE_API_KEY")
    crawler_llm_enabled: bool = Field(default=False, alias="CRAWLER_LLM_ENABLED")
    crawler_llm_model: str = Field(default="gpt-4o-mini", alias="CRAWLER_LLM_MODEL")
    crawler_llm_max_tokens: int = Field(default=2000, alias="CRAWLER_LLM_MAX_TOKENS")
    crawler_llm_max_calls_per_run: int = Field(default=3, alias="CRAWLER_LLM_MAX_CALLS_PER_RUN")
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

