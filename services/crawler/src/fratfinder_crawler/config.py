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
    crawler_search_enabled: bool = Field(default=True, alias="CRAWLER_SEARCH_ENABLED")
    crawler_search_provider: str = Field(default="auto", alias="CRAWLER_SEARCH_PROVIDER")
    crawler_search_max_results: int = Field(default=5, alias="CRAWLER_SEARCH_MAX_RESULTS")
    crawler_search_max_pages_per_job: int = Field(default=3, alias="CRAWLER_SEARCH_MAX_PAGES_PER_JOB")
    crawler_search_brave_api_key: str | None = Field(default=None, alias="CRAWLER_SEARCH_BRAVE_API_KEY")
    crawler_llm_enabled: bool = Field(default=False, alias="CRAWLER_LLM_ENABLED")
    crawler_llm_model: str = Field(default="gpt-4o-mini", alias="CRAWLER_LLM_MODEL")
    crawler_llm_max_tokens: int = Field(default=2000, alias="CRAWLER_LLM_MAX_TOKENS")
    crawler_llm_max_calls_per_run: int = Field(default=3, alias="CRAWLER_LLM_MAX_CALLS_PER_RUN")
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

