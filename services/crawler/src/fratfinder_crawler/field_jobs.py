from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import unquote, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

from fratfinder_crawler.candidate_sanitizer import (
    CandidateKind,
    sanitize_as_email,
    sanitize_as_instagram,
    sanitize_as_website,
    sanitize_candidate,
)
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import (
    ExtractedChapter,
    FieldJob,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_TO_STATE_KEY,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    ProvenanceRecord,
    ReviewItemCandidate,
    SourceRecord,
)
from fratfinder_crawler.normalization import normalize_record
from fratfinder_crawler.search import SearchClient, SearchResult, SearchUnavailableError

if TYPE_CHECKING:
    from fratfinder_crawler.db.repository import CrawlerRepository

_EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b")
_MAILTO_RE = re.compile(r"mailto:([^?\s]+)", re.IGNORECASE)
_INSTAGRAM_RE = re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.-]+", re.IGNORECASE)
_INSTAGRAM_PATH_RE = re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.-]+)", re.IGNORECASE)
_INSTAGRAM_HANDLE_HINT_RE = re.compile(
    r"(?:instagram|insta|ig)(?:\s*(?:[:\-]|handle|account|profile)\s*)@?([A-Za-z0-9_.]{2,30})",
    re.IGNORECASE,
)
_INSTAGRAM_NEARBY_HANDLE_RE = re.compile(
    r"(?:instagram|insta|ig)[^@A-Za-z0-9]{0,15}@([A-Za-z0-9_.]{2,30})",
    re.IGNORECASE,
)
_URL_RE = re.compile(r'https?://[^\s\]\[\)\("<>]+', re.IGNORECASE)
_OBFUSCATED_AT_RE = re.compile(r"\s*(?:@|\(at\)|\[at\]|\{at\}|\sat\s)\s*", re.IGNORECASE)
_OBFUSCATED_DOT_RE = re.compile(r"\s*(?:\.|\(dot\)|\[dot\]|\{dot\}|\sdot\s)\s*", re.IGNORECASE)
_GENERIC_EMAIL_PREFIXES = {"info", "contact", "admin", "office", "hello", "membership", "national", "headquarters"}
_IGNORED_INSTAGRAM_SEGMENTS = {"p", "reel", "tv", "stories", "explore", "accounts", "mailto"}
_DOCUMENT_URL_EXTENSIONS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".ics")
_BLOCKED_WEBSITE_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "facebook.com", "www.facebook.com", "instagram.com", "www.instagram.com", "twitter.com", "x.com", "youtube.com", "www.youtube.com", "linkedin.com", "www.linkedin.com", "bing.com", "www.bing.com", "stackoverflow.com", "www.stackoverflow.com", "stackexchange.com", "github.com", "www.github.com", "sigmaaldrich.com", "www.sigmaaldrich.com", "sigma-aldrich.com", "www.sigma-aldrich.com", "milliporesigma.com", "www.milliporesigma.com", "merckmillipore.com", "www.merckmillipore.com"}
_TIER2_WEBSITE_HOSTS = {"linktr.ee", "www.linktr.ee", "beacons.ai", "www.beacons.ai", "bio.site", "www.bio.site", "campsite.bio", "www.campsite.bio", "allmylinks.com", "www.allmylinks.com", "lnk.bio", "www.lnk.bio", "stan.store", "www.stan.store"}
_LOW_SIGNAL_INSTAGRAM_RESULT_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "dcurbanmom.com", "www.dcurbanmom.com", "worldscholarshipforum.com", "www.worldscholarshipforum.com", "sigmaaldrich.com", "www.sigmaaldrich.com", "sigma-aldrich.com", "www.sigma-aldrich.com", "milliporesigma.com", "www.milliporesigma.com", "merckmillipore.com", "www.merckmillipore.com"}
_LOW_SIGNAL_EMAIL_RESULT_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "facebook.com", "www.facebook.com", "instagram.com", "www.instagram.com", "x.com", "twitter.com", "www.twitter.com", "youtube.com", "www.youtube.com", "sigmaaldrich.com", "www.sigmaaldrich.com", "sigma-aldrich.com", "www.sigma-aldrich.com", "milliporesigma.com", "www.milliporesigma.com", "merckmillipore.com", "www.merckmillipore.com"}
_FREE_EMAIL_DOMAINS = {"gmail.com", "googlemail.com", "yahoo.com", "outlook.com", "hotmail.com", "live.com", "aol.com", "icloud.com", "me.com", "protonmail.com"}
_MATCH_STOPWORDS = {"university", "college", "campus", "chapter", "official", "site", "email", "contact", "instagram", "profile", "fraternity", "house", "the", "and", "for"}
_GREEK_LETTER_TOKENS = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega"}
_LOW_SIGNAL_AFFILIATION_MARKERS = (
    "admission",
    "admissions",
    "apply",
    "applying",
    "archive",
    "archives",
    "archivesspace",
    "article",
    "articles",
    "award",
    "awards",
    "book export",
    "calendar",
    "commencement",
    "encyclopedia",
    "event",
    "events",
    "fellow",
    "fellows",
    "fellowship",
    "history",
    "in memoriam",
    "magazine",
    "memorial",
    "news",
    "obit",
    "obits",
    "obituar",
    "our fellows",
    "prize",
    "prizes",
    "profile",
    "profiles",
    "publication",
    "publications",
    "review",
    "scholarship",
    "scholarships",
    "special collections",
    "student voices",
    "summer",
    "urology",
    "visiting writers",
    "voices",
)
_LOW_SIGNAL_WEBSITE_PATH_MARKERS = (
    "apparel",
    "article",
    "articles",
    "award",
    "awards",
    "blog",
    "bookstore",
    "calendar",
    "event",
    "events",
    "grade-report",
    "grade_report",
    "history",
    "journalism",
    "merch",
    "news",
    "onebook",
    "post",
    "posts",
    "prize",
    "prizes",
    "profile",
    "profiles",
    "report",
    "reports",
    "resource",
    "resources",
    "scholarship",
    "scholarships",
    "shop",
    "statistics",
    "store",
    "student-engagement",
    "story",
    "stories",
    "terminology",
    "statement",
    "statements",
    "trustees",
    "wordpress",
    "wp-content",
)
_OFFICIAL_AFFILIATION_MARKERS = (
    "chapter profile",
    "chapter profiles",
    "chapters",
    "clubs organizations",
    "council",
    "find a student org",
    "fraternities",
    "fraternity and sorority",
    "fraternity chapters",
    "fraternity sorority life",
    "greek life",
    "greek organizations",
    "ifc",
    "interfraternity",
    "organization profile",
    "organization scorecard",
    "recognized chapters",
    "student org",
    "student organization",
    "student organizations",
)
_WEBSITE_LINK_CUE_MARKERS = (
    "chapter website",
    "official website",
    "visit website",
    "visit site",
    "website",
    "homepage",
    "home page",
    "go to site",
    "chapter site",
    "site",
)
_EMAIL_ROLE_MARKERS = (
    "advisor",
    "board",
    "contact",
    "contacts",
    "email",
    "executive",
    "leadership",
    "officer",
    "officers",
    "president",
    "recruit",
    "recruitment",
    "rush",
    "secretary",
    "treasurer",
    "vice president",
)
_GENERIC_OFFICE_EMAIL_MARKERS = {
    "admissions",
    "advisor",
    "greeklife",
    "greek.life",
    "leadership",
    "office",
    "reslife",
    "studentaffairs",
    "student.life",
    "studentlife",
}
_FRATERNITY_NON_IDENTITY_TOKENS = {"main", "national", "nationals"}
_CHAPTER_SIGNAL_STOPWORDS = {
    "chapter",
    "colony",
    "active",
    "inactive",
    "associate",
    "associates",
    "provisional",
    "suspended",
    "rechartered",
    "interest",
    "group",
}
_INSTAGRAM_CONFLICT_MARKERS = {
    "tri sigma": "sigma sigma sigma",
    "sigma sigma sigma": "sigma sigma sigma",
    "trisigma": "sigma sigma sigma",
    "delta chi fraternity": "delta chi",
}
_GREEDY_COLLECT_NONE = "none"
_GREEDY_COLLECT_PASSIVE = "passive"
_GREEDY_COLLECT_BFS = "bfs"
_NATIONALS_LINK_MARKERS = (
    "chapter-directory",
    "chapters",
    "directory",
    "find-a-chapter",
    "findachapter",
    "locations",
    "locator",
    "state",
    "province",
)
_GENERIC_DIRECTORY_PATH_MARKERS = (
    "chapter-directory",
    "chapter-roll",
    "chapters",
    "directory",
    "find-a-chapter",
    "findachapter",
    "join-a-chapter",
    "join",
    "locations",
    "locator",
    "expansion",
    "our-chapters",
)
_SOCIAL_LABELS = ("facebook", "instagram", "twitter", "x.com", "linkedin")
_NATIONALS_HEADING_BLOCKLIST_MARKERS = (
    "directory",
    "chapter directory",
    "chapter finder",
    "chapter map",
    "chapter list",
    "chapter locations",
    "chapter locator",
    "chapter officers",
    "chapter house corporation",
    "chapter house",
    "chapter news",
    "chapter event",
    "chapter events",
    "chapter resources",
    "chapter history",
)
_NATIONALS_CONTACT_CUE_MARKERS = ("website", "instagram", "facebook", "twitter", "x.com", "@")
_NATIONALS_SCRIPT_URL_RE = re.compile(r"['\"]url['\"]\s*:\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
_STATE_ABBREVIATIONS = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga", "hi", "id", "il", "in", "ia", "ks",
    "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc",
    "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
    "ab", "bc", "mb", "nb", "nl", "ns", "nt", "nu", "on", "pe", "qc", "sk", "yt",
}
_DEFAULT_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


@dataclass(slots=True)
class FieldJobResult:
    chapter_updates: dict[str, str]
    completed_payload: dict[str, Any]
    field_state_updates: dict[str, str] = field(default_factory=dict)
    provenance_records: list[ProvenanceRecord] = field(default_factory=list)
    review_item: ReviewItemCandidate | None = None


@dataclass(slots=True)
class SearchDocument:
    text: str
    links: list[str] = field(default_factory=list)
    url: str | None = None
    title: str | None = None
    provider: str = "provenance"
    query: str | None = None
    html: str | None = None


@dataclass(slots=True)
class CandidateMatch:
    value: str
    confidence: float
    source_url: str
    source_snippet: str
    field_name: str
    source_provider: str = "provenance"
    related_website_url: str | None = None
    query: str | None = None


@dataclass(slots=True)
class NationalsChapterEntry:
    chapter_name: str
    university_name: str | None
    website_url: str | None
    instagram_url: str | None
    contact_email: str | None
    source_url: str
    source_snippet: str
    confidence: float


class FieldJobEngine:
    def __init__(
        self,
        repository: CrawlerRepository,
        logger,
        worker_id: str,
        base_backoff_seconds: int = 30,
        source_slug: str | None = None,
        head_requester: Callable[..., object] | None = None,
        get_requester: Callable[..., object] | None = None,
        search_client: SearchClient | None = None,
        search_provider: str | None = None,
        max_search_pages: int = 3,
        negative_result_cooldown_days: int = 30,
        dependency_wait_seconds: int = 300,
        email_max_queries: int = 5,
        instagram_max_queries: int = 6,
        enable_school_initials: bool = True,
        min_school_initial_length: int = 3,
        enable_compact_fraternity: bool = True,
        instagram_enable_handle_queries: bool = True,
        instagram_direct_probe_enabled: bool = False,
        require_confident_website_for_email: bool = True,
        email_escape_on_provider_block: bool = True,
        email_escape_min_website_failures: int = 2,
        transient_short_retries: int = 2,
        transient_long_cooldown_seconds: int = 900,
        min_no_candidate_backoff_seconds: int = 60,
        greedy_collect_mode: str = _GREEDY_COLLECT_NONE,
        field_name: str | None = None,
    ):
        self._repository = repository
        self._logger = logger
        self._worker_id = worker_id
        self._base_backoff_seconds = max(1, base_backoff_seconds)
        self._source_slug = source_slug
        self._search_client = search_client
        self._search_provider = (search_provider or self._detect_search_provider(search_client)).lower()
        self._max_search_pages = max(1, max_search_pages)
        self._negative_result_cooldown_seconds = max(0, negative_result_cooldown_days) * 24 * 60 * 60
        self._dependency_wait_seconds = max(0, dependency_wait_seconds)
        self._email_max_queries = max(1, email_max_queries)
        self._instagram_max_queries = max(1, instagram_max_queries)
        self._enable_school_initials = enable_school_initials
        self._min_school_initial_length = max(2, min_school_initial_length)
        self._enable_compact_fraternity = enable_compact_fraternity
        self._instagram_enable_handle_queries = instagram_enable_handle_queries
        self._instagram_direct_probe_enabled = instagram_direct_probe_enabled
        self._require_confident_website_for_email = require_confident_website_for_email
        self._email_escape_on_provider_block = email_escape_on_provider_block
        self._email_escape_min_website_failures = max(1, email_escape_min_website_failures)
        self._transient_short_retries = max(0, transient_short_retries)
        self._transient_long_cooldown_seconds = max(0, transient_long_cooldown_seconds)
        self._min_no_candidate_backoff_seconds = max(0, min_no_candidate_backoff_seconds)
        self._greedy_collect_mode = _normalize_greedy_collect_mode(greedy_collect_mode)
        self._field_name = field_name
        self._search_errors_encountered = False
        self._search_queries_attempted = 0
        self._search_queries_failed = 0
        self._search_queries_succeeded = 0
        self._search_fanout_aborted = False
        self._last_provider_attempts: list[dict[str, object]] = []
        self._last_query_provider_attempts: list[dict[str, object]] = []
        self._last_search_failure_kind: str | None = None
        self._search_result_cache: dict[str, list[SearchResult]] = {}
        self._search_document_cache: dict[str, SearchDocument | None] = {}
        self._provenance_text_cache: dict[str, str] = {}
        self._candidate_rejection_counts: dict[str, int] = {}
        self._decision_trace: list[dict[str, str | int | float | bool | None]] = []
        self._nationals_entries_cache: dict[str, list[NationalsChapterEntry]] = {}
        self._nationals_collect_attempted: set[str] = set()
        self._source_record_cache: dict[str, SourceRecord | None] = {}
        search_settings = getattr(search_client, "_settings", None)
        configured_user_agent = str(getattr(search_settings, "crawler_http_user_agent", "") or "").strip()
        if configured_user_agent.lower().startswith("fratfinderai/") or not configured_user_agent:
            configured_user_agent = _DEFAULT_BROWSER_USER_AGENT
        self._http_headers = {
            "User-Agent": configured_user_agent,
            "Accept-Language": "en-US,en;q=0.9",
        }
        self._http_verify_ssl = bool(getattr(search_settings, "crawler_http_verify_ssl", True))
        self._http_session: requests.Session | None = None
        if head_requester is None or get_requester is None:
            self._http_session = requests.Session()
            adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
            self._http_session.mount("http://", adapter)
            self._http_session.mount("https://", adapter)
        self._head_requester = head_requester or self._default_head_requester
        self._get_requester = get_requester or self._default_get_requester
        self._cache_empty_search_results = bool(getattr(search_settings, "crawler_search_cache_empty_results", False))

    def _default_head_requester(self, url: str, *, timeout: float = 10, allow_redirects: bool = True, **kwargs):
        if self._http_session is None:
            self._http_session = requests.Session()
            adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
            self._http_session.mount("http://", adapter)
            self._http_session.mount("https://", adapter)
        headers = dict(kwargs.pop("headers", {}) or {})
        for key, value in self._http_headers.items():
            headers.setdefault(key, value)
        kwargs.setdefault("verify", self._http_verify_ssl)
        return self._http_session.head(
            url,
            timeout=timeout,
            allow_redirects=allow_redirects,
            headers=headers,
            **kwargs,
        )

    def _default_get_requester(self, url: str, *, timeout: float = 10, allow_redirects: bool = True, **kwargs):
        if self._http_session is None:
            self._http_session = requests.Session()
            adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
            self._http_session.mount("http://", adapter)
            self._http_session.mount("https://", adapter)
        headers = dict(kwargs.pop("headers", {}) or {})
        for key, value in self._http_headers.items():
            headers.setdefault(key, value)
        kwargs.setdefault("verify", self._http_verify_ssl)
        return self._http_session.get(
            url,
            timeout=timeout,
            allow_redirects=allow_redirects,
            headers=headers,
            **kwargs,
        )

    def process(self, limit: int = 25) -> dict[str, int]:
        processed = 0
        requeued = 0
        failed_terminal = 0

        for _ in range(limit):
            job = self._repository.claim_next_field_job(
                self._worker_id,
                source_slug=self._source_slug,
                field_name=self._field_name,
                require_confident_website_for_email=False,
            )
            if job is None:
                break

            log_event(
                self._logger,
                "field_job_claimed",
                worker_id=self._worker_id,
                field_job_id=job.id,
                chapter_slug=job.chapter_slug,
                field_name=job.field_name,
                attempts=job.attempts,
                max_attempts=job.max_attempts,
            )

            try:
                result = self._process_single_job(job)
                if result.review_item is not None:
                    self._repository.create_field_job_review_item(job, result.review_item)
                self._repository.complete_field_job(
                    job,
                    result.chapter_updates,
                    result.completed_payload,
                    result.field_state_updates,
                    result.provenance_records,
                )
                processed += 1
                log_event(
                    self._logger,
                    "field_job_completed",
                    field_job_id=job.id,
                    chapter_slug=job.chapter_slug,
                    field_name=job.field_name,
                    updates=result.chapter_updates,
                    field_states=result.field_state_updates,
                )
            except RetryableJobError as exc:
                retry_limit = self._retry_limit(job, exc)
                if not exc.preserve_attempt and job.attempts >= retry_limit:
                    self._repository.fail_field_job_terminal(job, str(exc))
                    failed_terminal += 1
                    log_event(
                        self._logger,
                        "field_job_terminal_failure",
                        field_job_id=job.id,
                        chapter_slug=job.chapter_slug,
                        field_name=job.field_name,
                        error=str(exc),
                        retry_reason=exc.reason_code,
                    )
                    continue

                backoff_seconds = exc.backoff_seconds if exc.backoff_seconds is not None else self._base_backoff_seconds * (2 ** (job.attempts - 1))
                payload_patch = self._build_requeue_payload_patch(job, exc, backoff_seconds)
                self._repository.requeue_field_job(
                    job,
                    str(exc),
                    backoff_seconds,
                    preserve_attempt=exc.preserve_attempt,
                    payload_patch=payload_patch,
                )
                requeued += 1
                log_event(
                    self._logger,
                    "field_job_requeued",
                    field_job_id=job.id,
                    chapter_slug=job.chapter_slug,
                    field_name=job.field_name,
                    backoff_seconds=backoff_seconds,
                    error=str(exc),
                    retry_reason=exc.reason_code,
                    search_queries_attempted=self._search_queries_attempted,
                    search_queries_succeeded=self._search_queries_succeeded,
                    search_queries_failed=self._search_queries_failed,
                    transient_provider_failures=payload_patch.get("transient_provider_failures"),
                )
            except Exception as exc:  # pragma: no cover - guardrail path
                self._repository.fail_field_job_terminal(job, str(exc))
                failed_terminal += 1
                log_event(
                    self._logger,
                    "field_job_unexpected_failure",
                    field_job_id=job.id,
                    chapter_slug=job.chapter_slug,
                    field_name=job.field_name,
                    error=str(exc),
                )

        return {
            "processed": processed,
            "requeued": requeued,
            "failed_terminal": failed_terminal,
        }

    def process_claimed_job(self, job: FieldJob) -> FieldJobResult:
        return self._process_single_job(job)

    def _process_single_job(self, job: FieldJob) -> FieldJobResult:
        self._search_errors_encountered = False
        self._search_queries_attempted = 0
        self._search_queries_failed = 0
        self._search_queries_succeeded = 0
        self._search_fanout_aborted = False
        self._last_provider_attempts = []
        self._last_query_provider_attempts = []
        self._last_search_failure_kind = None
        self._candidate_rejection_counts = {}
        self._decision_trace = []
        self._trace(
            "job_started",
            field_name=job.field_name,
            chapter_slug=job.chapter_slug,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            has_website=bool(_current_website_url(job)),
            has_email=bool(job.contact_email),
            has_instagram=bool(job.instagram_url),
        )
        self._trace("load_context", source_slug=job.source_slug, source_base_url=bool(job.source_base_url), university_name=job.university_name)

        if job.field_name == FIELD_JOB_FIND_EMAIL:
            if job.contact_email:
                self._trace("already_populated", target="contact_email")
                return self._already_populated_result(job.field_name, job.contact_email)
            admission_result = self._admission_gate(job, "contact_email", allow_confident_website=True)
            if admission_result is not None:
                return admission_result
            if self._requires_website_first(job) and not _website_is_confident(job):
                self._trace("dependency_wait", reason="website_not_confident")
                raise RetryableJobError(
                    "Waiting for confident website discovery before email enrichment",
                    backoff_seconds=self._dependency_wait_seconds,
                    preserve_attempt=True,
                    reason_code="dependency_wait",
                )
            match = self._find_email_candidate(job)
            if match is None:
                if self._should_complete_provider_degraded(job):
                    self._trace("provider_degraded", target="contact_email")
                    return self._provider_degraded_result(job, "contact_email")
                self._trace("no_candidate", target="contact_email")
                self._emit_candidate_rejection_summary(job, target="email")
                raise self._no_candidate_error(job, "No candidate email found in provenance, chapter website, or search results")
            self._trace("candidate_selected", target="contact_email", confidence=round(match.confidence, 4), provider=match.source_provider)
            return self._candidate_result(job, match, "contact_email")

        if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
            if job.instagram_url:
                self._trace("already_populated", target="instagram_url")
                return self._already_populated_result(job.field_name, job.instagram_url)
            admission_result = self._admission_gate(job, "instagram_url")
            if admission_result is not None:
                return admission_result
            match = self._find_instagram_candidate(job)
            if match is not None:
                self._trace("candidate_selected", target="instagram_url", confidence=round(match.confidence, 4), provider=match.source_provider)
                return self._candidate_result(job, match, "instagram_url")
            fallback_result = self._resolve_instagram_search_miss(job)
            if fallback_result is not None:
                self._trace("inactive_resolution", target="instagram_url")
                return fallback_result
            if self._should_complete_provider_degraded(job):
                self._trace("provider_degraded", target="instagram_url")
                return self._provider_degraded_result(job, "instagram_url")
            self._trace("no_candidate", target="instagram_url")
            self._emit_candidate_rejection_summary(job, target="instagram")
            raise self._no_candidate_error(job, "No candidate instagram URL found in provenance, chapter website, or search results")

        if job.field_name == FIELD_JOB_FIND_WEBSITE:
            existing_website = _current_website_url(job)
            if existing_website:
                self._trace("already_populated", target="website_url")
                return self._already_populated_result(job.field_name, existing_website)
            admission_result = self._admission_gate(job, "website_url")
            if admission_result is not None:
                return admission_result
            match = self._find_website_candidate(job)
            if match is None:
                if self._should_complete_provider_degraded(job):
                    self._trace("provider_degraded", target="website_url")
                    return self._provider_degraded_result(job, "website_url")
                self._trace("no_candidate", target="website_url")
                self._emit_candidate_rejection_summary(job, target="website")
                raise self._no_candidate_error(job, "No candidate website URL available")
            self._trace("candidate_selected", target="website_url", confidence=round(match.confidence, 4), provider=match.source_provider)
            return self._candidate_result(job, match, "website_url")

        if job.field_name == FIELD_JOB_VERIFY_WEBSITE:
            return self._verify_website(job)

        if job.field_name == FIELD_JOB_VERIFY_SCHOOL:
            return self._verify_school_match(job)

        raise RetryableJobError(f"Unsupported field job type: {job.field_name}", reason_code="dependency_wait")

    def _admission_gate(
        self,
        job: FieldJob,
        target_field: str,
        *,
        allow_confident_website: bool = False,
    ) -> FieldJobResult | None:
        self._trace("admission_gate", target=target_field)
        if self._payload_int(job.payload.get("terminal_no_signal_count")) > 0 and int(job.attempts or 0) > 1:
            return self._terminal_no_signal_result(job, target_field, reason_code="cached_no_signal")
        chapter_name = (job.chapter_name or "").strip()
        school = (job.university_name or "").strip()
        school_ok = bool(school) and not _is_low_signal_university_name(school)
        chapter_ok = bool(chapter_name)
        if chapter_ok and (school_ok or (allow_confident_website and _website_is_confident(job))):
            return None
        return self._terminal_no_signal_result(job, target_field, reason_code="not_enough_identity")

    def _terminal_no_signal_result(self, job: FieldJob, target_field: str, *, reason_code: str) -> FieldJobResult:
        state_key = target_field
        return FieldJobResult(
            chapter_updates={},
            completed_payload={
                "status": "terminal_no_signal",
                "field": target_field,
                "reasonCode": reason_code,
                "decision_trace": self._build_decision_trace_summary(),
                "rejection_summary": self._candidate_rejection_summary_payload(),
            },
            field_state_updates={state_key: "missing"},
        )

    def _provider_degraded_result(self, job: FieldJob, target_field: str) -> FieldJobResult:
        state_key = target_field
        return FieldJobResult(
            chapter_updates={},
            completed_payload={
                "status": "provider_degraded",
                "field": target_field,
                "reasonCode": "provider_degraded",
                "decision_trace": self._build_decision_trace_summary(),
                "rejection_summary": self._candidate_rejection_summary_payload(),
            },
            field_state_updates={state_key: job.field_states.get(state_key, "missing") or "missing"},
        )

    def _should_complete_provider_degraded(self, job: FieldJob) -> bool:
        if not self._search_errors_encountered:
            return False
        if self._search_queries_attempted <= 0 or self._search_queries_failed < self._search_queries_attempted:
            return False
        queue_state = ((job.payload.get("contactResolution") or {}).get("queueState") if isinstance(job.payload.get("contactResolution"), dict) else None) or "actionable"
        return queue_state == "deferred" and self._payload_int(job.payload.get("transient_provider_failures")) >= self._transient_short_retries and int(job.attempts or 0) > 1

    def _candidate_result(self, job: FieldJob, match: CandidateMatch, target_field: str) -> FieldJobResult:
        expected_kind = {
            "website_url": CandidateKind.WEBSITE,
            "contact_email": CandidateKind.EMAIL,
            "instagram_url": CandidateKind.INSTAGRAM,
        }.get(target_field)
        if expected_kind is None:
            raise RuntimeError(f"Unsupported target field for candidate result: {target_field}")
        sanitized = sanitize_candidate(
            match.value,
            expected=expected_kind,
            base_url=match.source_url or job.source_base_url,
        )
        if sanitized is None:
            self._trace("candidate_rejected", target=target_field, reason="sanitizer_invalid_value")
            raise self._no_candidate_error(job, f"Candidate for {target_field} failed sanitizer validation")
        if sanitized.kind != expected_kind:
            self._trace(
                "candidate_rerouted",
                target=target_field,
                rerouted_to=sanitized.kind.value,
                original_kind=sanitized.original_kind.value,
            )
            raise self._no_candidate_error(job, f"Candidate kind mismatch for {target_field}; discovered {sanitized.kind.value}")
        match.value = sanitized.value

        base_source_slug = job.source_slug or job.payload.get("sourceSlug") or "search-enrichment"
        provenance_records = [
            ProvenanceRecord(
                source_slug=base_source_slug,
                source_url=match.source_url,
                field_name=target_field,
                field_value=match.value,
                source_snippet=match.source_snippet[:400],
                confidence=match.confidence,
            )
        ]

        write_threshold = self._write_threshold(job, target_field, match)
        if match.confidence < write_threshold:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={
                    "status": "review_required",
                    "value": match.value,
                    "confidence": f"{match.confidence:.2f}",
                    "source_url": match.source_url,
                    "query": match.query,
                    "provider": match.source_provider,
                    "decision_trace": self._build_decision_trace_summary(),
                    "rejection_summary": self._candidate_rejection_summary_payload(),
                },
                provenance_records=provenance_records,
                review_item=ReviewItemCandidate(
                    item_type="search_candidate_review",
                    reason=f"Search enrichment found only a low-confidence candidate for {target_field}",
                    source_slug=job.source_slug,
                    chapter_slug=job.chapter_slug,
                    payload={
                        "fieldName": target_field,
                        "candidateValue": match.value,
                        "confidence": match.confidence,
                        "sourceUrl": match.source_url,
                        "extractionNotes": match.source_snippet,
                        "query": match.query,
                        "provider": match.source_provider,
                        "decisionTrace": self._build_decision_trace_summary(),
                        "rejectionSummary": self._candidate_rejection_summary_payload(),
                    },
                ),
            )

        found_threshold = self._found_threshold(job, target_field, match)
        field_state = "found" if match.confidence >= found_threshold else "low_confidence"
        chapter_updates = {target_field: match.value}
        field_state_updates = {target_field: field_state}
        if target_field != "website_url" and match.related_website_url and _is_safe_related_website_url(job, match.related_website_url):
            sanitized_related_website = sanitize_as_website(match.related_website_url, base_url=match.source_url or job.source_base_url)
            current_website = _current_website_url(job)
            if sanitized_related_website and (not current_website or current_website == sanitized_related_website):
                chapter_updates["website_url"] = sanitized_related_website
                field_state_updates["website_url"] = "found" if match.confidence >= found_threshold else "low_confidence"

        completed_payload = {
            "status": "updated",
            target_field: match.value,
            "confidence": f"{match.confidence:.2f}",
            "source_url": match.source_url,
            "provider": match.source_provider,
            "decision_trace": self._build_decision_trace_summary(),
        }
        if match.query:
            completed_payload["query"] = match.query
        if match.related_website_url and target_field != "website_url":
            completed_payload["related_website_url"] = match.related_website_url

        if target_field != "website_url" and chapter_updates.get("website_url"):
            provenance_records.append(
                ProvenanceRecord(
                    source_slug=base_source_slug,
                    source_url=match.source_url,
                    field_name="website_url",
                    field_value=chapter_updates["website_url"],
                    source_snippet=match.source_snippet[:400],
                    confidence=min(0.9, match.confidence),
                )
            )

        return FieldJobResult(
            chapter_updates=chapter_updates,
            completed_payload=completed_payload,
            field_state_updates=field_state_updates,
            provenance_records=provenance_records,
        )

    def _verify_website(self, job: FieldJob) -> FieldJobResult:
        current_website = _current_website_url(job)
        if not current_website:
            raise RetryableJobError(
                "No website URL available to verify",
                backoff_seconds=self._dependency_wait_seconds,
                preserve_attempt=True,
                reason_code="dependency_wait",
            )

        try:
            response = self._head_requester(current_website, timeout=10, allow_redirects=True)
        except requests.Timeout as exc:
            raise RetryableJobError("Website verification timed out", reason_code="transient_network") from exc
        except requests.RequestException as exc:
            raise RetryableJobError(f"Website verification request failed: {exc}", reason_code="transient_network") from exc

        status_code = getattr(response, "status_code", None)
        if status_code is None:
            raise RetryableJobError("Website verification did not return an HTTP status code", reason_code="transient_network")
        verification_method = "head"
        if status_code in {401, 403, 405, 406, 429}:
            try:
                get_response = self._get_requester(current_website, timeout=10, allow_redirects=True)
            except requests.Timeout as exc:
                raise RetryableJobError("Website verification timed out", reason_code="transient_network") from exc
            except requests.RequestException as exc:
                raise RetryableJobError(f"Website verification request failed: {exc}", reason_code="transient_network") from exc
            get_status_code = getattr(get_response, "status_code", None)
            if get_status_code is not None:
                status_code = get_status_code
                verification_method = "get"
        if 200 <= status_code < 400:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={
                    "status": "verified",
                    "website_url": current_website,
                    "status_code": str(status_code),
                    "verification_method": verification_method,
                    "decision_trace": self._build_decision_trace_summary(),
                },
                field_state_updates={"website_url": "found"},
            )
        if 400 <= status_code < 500:
            raise RetryableJobError(f"Website verification returned client error status {status_code}", reason_code="provider_low_signal")
        raise RetryableJobError(f"Website verification returned server error status {status_code}", reason_code="transient_network")

    def _verify_school_match(self, job: FieldJob) -> FieldJobResult:
        chapter_school = _slugify(job.university_name)
        candidate_school = _slugify(job.payload.get("candidateSchoolName"))
        if chapter_school and candidate_school and chapter_school == candidate_school:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={"status": "verified", "university_name": job.university_name or ""},
                field_state_updates={"university_name": "found"},
            )
        if chapter_school and candidate_school and chapter_school != candidate_school:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={
                    "status": "mismatch_reviewed",
                    "stored_university_name": job.university_name or "",
                    "candidate_school_name": str(job.payload.get("candidateSchoolName") or ""),
                },
                review_item=ReviewItemCandidate(
                    item_type="school_match_mismatch",
                    reason="Candidate school name does not match the stored university name",
                    source_slug=job.payload.get("sourceSlug") if isinstance(job.payload.get("sourceSlug"), str) else None,
                    chapter_slug=job.chapter_slug,
                    payload={
                        "storedUniversityName": job.university_name,
                        "candidateSchoolName": job.payload.get("candidateSchoolName"),
                    },
                ),
            )
        raise RetryableJobError("Insufficient school data to verify school match", reason_code="dependency_wait")

    def _find_email_candidate(self, job: FieldJob) -> CandidateMatch | None:
        matches: list[CandidateMatch] = []

        self._trace("email_strategy", stage="provenance")
        provenance_match = self._find_email_candidate_from_provenance(job)
        if provenance_match is not None:
            matches.append(provenance_match)

        self._trace("email_strategy", stage="chapter_website")
        website_matches = self._extract_email_matches_from_website(job)
        matches.extend(website_matches)
        best_local = _best_match(matches)
        if best_local is not None and best_local.confidence >= self._found_threshold(job, "contact_email", best_local):
            return best_local

        if not _website_is_confident(job):
            self._trace("email_strategy", stage="trusted_school_pages")
            trusted_school_matches = self._extract_email_matches_from_trusted_school_pages(job)
            matches.extend(trusted_school_matches)
            best_school = _best_match(matches)
            if best_school is not None and best_school.confidence >= self._found_threshold(job, "contact_email", best_school):
                return best_school

        self._trace("email_strategy", stage="nationals")
        nationals_matches = self._find_target_candidates_from_nationals(job, target="email")
        if nationals_matches:
            matches.extend(nationals_matches)
            best_nationals = _best_match(matches)
            if best_nationals is not None and best_nationals.confidence >= self._found_threshold(job, "contact_email", best_nationals):
                return best_nationals

        self._trace("email_strategy", stage="search")
        for document in self._search_documents(job, target="email", include_existing=False):
            document_matches = self._extract_email_matches(document, job)
            if not document_matches:
                continue
            matches.extend(document_matches)
            best_external = _best_match(matches)
            if best_external is not None and best_external.confidence >= max(0.9, self._found_threshold(job, "contact_email", best_external)):
                return best_external

        return _best_match(matches)
    def _find_instagram_candidate(self, job: FieldJob) -> CandidateMatch | None:
        matches: list[CandidateMatch] = []
        current_website = _current_website_url(job)

        if current_website and _website_trust_tier(job, current_website) == "tier1":
            self._trace("instagram_strategy", stage="trusted_chapter_website")
            website_document = self._fetch_search_document(current_website, provider="chapter_website")
            if website_document is not None:
                website_matches = self._extract_instagram_matches(website_document, job)
                matches.extend(website_matches)
                best_website = _best_match(website_matches)
                if best_website is not None and best_website.confidence >= self._found_threshold(job, "instagram_url", best_website):
                    return best_website

        self._trace("instagram_strategy", stage="nationals")
        nationals_matches = self._find_target_candidates_from_nationals(job, target="instagram")
        if nationals_matches:
            matches.extend(nationals_matches)
            best_nationals = _best_match(matches)
            if best_nationals is not None and best_nationals.confidence >= self._found_threshold(job, "instagram_url", best_nationals):
                return best_nationals

        self._trace("instagram_strategy", stage="search")
        for document in self._search_documents(job, target="instagram", include_existing=False):
            document_matches = self._extract_instagram_matches(document, job)
            if not document_matches:
                continue
            matches.extend(document_matches)
            best_external = _best_match(matches)
            if best_external is not None and best_external.confidence >= max(0.9, self._found_threshold(job, "instagram_url", best_external)):
                return best_external

        best_external = _best_match(matches)
        if best_external is not None and best_external.confidence >= 0.88:
            return best_external

        if job.source_base_url:
            self._trace("instagram_strategy", stage="source_page")
            source_document = self._fetch_search_document(job.source_base_url, provider="source_page")
            if source_document is not None:
                source_matches = self._extract_instagram_matches(source_document, job)
                matches.extend(source_matches)
                best_source = _best_match(matches)
                if best_source is not None and best_source.confidence >= self._found_threshold(job, "instagram_url", best_source):
                    return best_source

        self._trace("instagram_strategy", stage="provenance")
        provenance_match = self._find_instagram_candidate_from_provenance(job)
        if provenance_match is not None:
            matches.append(provenance_match)

        if self._instagram_direct_probe_enabled and self._search_queries_succeeded == 0 and self._search_queries_failed > 0:
            self._trace("instagram_strategy", stage="direct_handle_probe")
            probe_matches = self._probe_instagram_handle_candidates(job)
            if probe_matches:
                matches.extend(probe_matches)
                best_probe = _best_match(matches)
                if best_probe is not None and best_probe.confidence >= self._found_threshold(job, "instagram_url", best_probe):
                    return best_probe

        return _best_match(matches)

    def _find_email_candidate_from_provenance(self, job: FieldJob) -> CandidateMatch | None:
        provenance_document = SearchDocument(text=self._source_text(job), provider="provenance", url=job.source_base_url)
        return _best_match(self._extract_email_matches(provenance_document, job))

    def _extract_email_matches_from_website(self, job: FieldJob) -> list[CandidateMatch]:
        current_website = _current_website_url(job)
        if not current_website:
            return []

        homepage_document = self._fetch_search_document(current_website, provider="chapter_website")
        if homepage_document is None:
            return []

        matches = self._extract_email_matches(homepage_document, job)
        followup_links = _email_followup_links(homepage_document, current_website, limit=self._max_search_pages)
        for link in followup_links:
            followup_document = self._fetch_search_document(link, provider="chapter_website")
            if followup_document is None:
                continue
            matches.extend(self._extract_email_matches(followup_document, job))
        return matches

    def _extract_email_matches_from_trusted_school_pages(self, job: FieldJob) -> list[CandidateMatch]:
        matches: list[CandidateMatch] = []
        seen_urls: set[str] = set()
        fetched_pages = 0
        query_limit = min(2, max(1, self._email_max_queries))
        for query in self._build_search_queries(job, target="website_school")[:query_limit]:
            for result in self._run_search(query):
                if result.url in seen_urls:
                    continue
                seen_urls.add(result.url)
                if _website_trust_tier(job, result.url) != "tier1":
                    continue
                if not _search_result_is_useful(job, result, target="email"):
                    continue
                snippet_document = SearchDocument(
                    text=result.snippet,
                    links=[result.url],
                    url=result.url,
                    title=result.title,
                    provider="search_result",
                    query=query,
                )
                if self._trusted_school_email_document(job, snippet_document):
                    matches.extend(self._extract_email_matches(snippet_document, job))

                if fetched_pages >= self._max_search_pages or _should_skip_search_page_fetch(result.url):
                    continue
                fetched_document = self._fetch_search_document(result.url, provider="search_page", query=query)
                if fetched_document is None or not self._trusted_school_email_document(job, fetched_document):
                    continue
                matches.extend(self._extract_email_matches(fetched_document, job))
                fetched_pages += 1

                best_match = _best_match(matches)
                if best_match is not None and best_match.confidence >= self._found_threshold(job, "contact_email", best_match):
                    return [best_match]
        return matches

    def _trusted_school_email_document(self, job: FieldJob, document: SearchDocument) -> bool:
        if document.provider == "search_page":
            return _school_affiliation_document_is_trusted(job, document)
        if document.provider == "search_result":
            if _website_trust_tier(job, document.url or "") != "tier1":
                return False
            combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or ""] if part))
            if not _school_matches(job, combined):
                return False
            if _fraternity_matches(job, combined) or _chapter_matches(job, combined):
                return True
            return any(marker in combined for marker in ("ifc", "greek", "fraternity", "student organization", "chapter"))
        return False
    def _find_instagram_candidate_from_provenance(self, job: FieldJob) -> CandidateMatch | None:
        provenance_document = SearchDocument(text=self._source_text(job), provider="provenance", url=job.source_base_url)
        return _best_match(self._extract_instagram_matches(provenance_document, job))

    def _probe_instagram_handle_candidates(self, job: FieldJob) -> list[CandidateMatch]:
        matches: list[CandidateMatch] = []
        school = job.university_name or str(job.payload.get("candidateSchoolName") or "")
        strong_school_aliases = _school_aliases(
            school,
            enable_school_initials=self._enable_school_initials,
            min_school_initial_length=self._min_school_initial_length,
        )
        if not strong_school_aliases:
            return []

        for handle in _instagram_probe_handles(
            job,
            enable_school_initials=self._enable_school_initials,
            min_school_initial_length=self._min_school_initial_length,
            enable_compact_fraternity=self._enable_compact_fraternity,
            max_candidates=max(4, self._instagram_max_queries),
        ):
            profile_url = f"https://www.instagram.com/{handle}/"
            document = self._fetch_search_document(
                profile_url,
                provider="instagram_probe",
                query=f"instagram_probe:{handle}",
            )
            if document is None:
                self._record_candidate_rejection("instagram", "probe_fetch_failed")
                continue
            if _instagram_profile_looks_missing(document):
                self._record_candidate_rejection("instagram", "probe_profile_missing")
                continue

            normalized = sanitize_as_instagram(profile_url)
            if not normalized:
                continue
            if _instagram_looks_institutional_or_directory_account(normalized, document) and not _instagram_handle_has_fraternity_token(normalized, job):
                self._record_candidate_rejection("instagram", "institutional_account")
                continue

            confidence = self._score_instagram_candidate(normalized, document, job, direct_url=True)
            if confidence < 0.80:
                self._record_candidate_rejection("instagram", "probe_low_confidence")
                continue
            matches.append(
                CandidateMatch(
                    value=normalized,
                    confidence=confidence,
                    source_url=document.url or normalized,
                    source_snippet=document.text[:400],
                    field_name="instagram_url",
                    source_provider="instagram_probe",
                    query=document.query,
                )
            )

            extracted_matches = self._extract_instagram_matches(document, job)
            if extracted_matches:
                matches.extend(extracted_matches)

            best = _best_match(matches)
            if best is not None and best.confidence >= 0.93:
                return [best]
        return matches

    def _resolve_instagram_search_miss(self, job: FieldJob) -> FieldJobResult | None:
        inactive_document = self._find_inactive_affiliation_document(job)
        if inactive_document is None:
            return None
        snippet = inactive_document.text[:400]
        return FieldJobResult(
            chapter_updates={"chapter_status": "inactive"},
            completed_payload={
                "status": "inactive_by_school_validation",
                "source_url": inactive_document.url or (job.source_base_url or "search-enrichment"),
                "reason": "official_school_affiliation_page_does_not_list_chapter",
                "decision_trace": self._build_decision_trace_summary(),
            },
            field_state_updates={"instagram_url": "missing"},
            provenance_records=[
                ProvenanceRecord(
                    source_slug=job.source_slug or str(job.payload.get("sourceSlug") or ""),
                    source_url=inactive_document.url or (job.source_base_url or "search-enrichment"),
                    field_name="chapter_status",
                    field_value="inactive",
                    source_snippet=snippet,
                    confidence=0.95,
                )
            ],
        )

    def _find_inactive_affiliation_document(self, job: FieldJob) -> SearchDocument | None:
        inactive_document: SearchDocument | None = None
        for document in self._search_documents(job, target="affiliation", include_existing=False):
            if not _school_affiliation_document_is_trusted(job, document):
                continue
            combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1600], document.url or ""] if part))
            if _fraternity_matches(job, combined):
                return None
            if inactive_document is None and _looks_like_official_school_affiliation_page(document):
                inactive_document = document
        return inactive_document

    def _find_website_candidate(self, job: FieldJob) -> CandidateMatch | None:
        matches: list[CandidateMatch] = []
        provenance_document = SearchDocument(
            text=self._source_text(job),
            provider="provenance",
            url=job.source_base_url,
        )
        self._trace("website_strategy", stage="provenance")
        matches.extend(self._extract_website_matches(provenance_document, job))
        self._trace("website_strategy", stage="website_school_search")
        for document in self._search_documents(job, target="website_school", include_existing=False):
            matches.extend(self._extract_website_matches(document, job))
        self._trace("website_strategy", stage="nationals")
        nationals_matches = self._find_target_candidates_from_nationals(job, target="website")
        matches.extend(nationals_matches)
        best = _best_match(matches)
        if best is not None:
            return best
        self._trace("website_strategy", stage="website_fallback_search")
        for document in self._search_documents(job, target="website_fallback", include_existing=False):
            matches.extend(self._extract_website_matches(document, job))
        return _best_match(matches)

    def _find_target_candidates_from_nationals(self, job: FieldJob, *, target: str) -> list[CandidateMatch]:
        if not job.source_base_url or not job.fraternity_slug:
            return []
        entries = self._get_nationals_entries(job)
        if not entries:
            return []
        if self._greedy_collect_mode != _GREEDY_COLLECT_NONE:
            self._maybe_ingest_nationals_entries(job, entries)

        matches: list[CandidateMatch] = []
        for entry in entries:
            if _nationals_entry_match_score(job, entry) < 2:
                continue
            document = _nationals_entry_to_document(entry)
            if target == "email":
                matches.extend(self._extract_email_matches(document, job))
                continue
            if target == "instagram":
                matches.extend(self._extract_instagram_matches(document, job))
                continue
            if target == "website":
                matches.extend(self._extract_website_matches(document, job))
        if matches:
            log_event(
                self._logger,
                "nationals_target_candidates_found",
                chapter_slug=job.chapter_slug,
                field_name=job.field_name,
                target=target,
                candidate_count=len(matches),
            )
        return matches

    def _get_nationals_entries(self, job: FieldJob) -> list[NationalsChapterEntry]:
        cache_key = f"{job.source_slug or job.fraternity_slug or ''}:{self._greedy_collect_mode}"
        cached = self._nationals_entries_cache.get(cache_key)
        if cached is not None:
            return cached
        entries = self._collect_nationals_entries(job)
        self._nationals_entries_cache[cache_key] = entries
        if entries:
            log_event(
                self._logger,
                "nationals_entries_collected",
                chapter_slug=job.chapter_slug,
                field_name=job.field_name,
                source_slug=job.source_slug,
                mode=self._greedy_collect_mode,
                entry_count=len(entries),
            )
        return entries

    def _collect_nationals_entries(self, job: FieldJob) -> list[NationalsChapterEntry]:
        base_url = job.source_base_url or ""
        source_host = (urlparse(base_url).netloc or "").lower()
        if not source_host:
            return []
        source_record = self._load_source_record(job.source_slug) if job.source_slug else None
        source_list_url = _source_list_url_for_job(job, source_record)

        if self._greedy_collect_mode == _GREEDY_COLLECT_BFS:
            max_pages = 24
            max_depth = 2
        else:
            max_pages = 8
            max_depth = 1

        seed_urls: list[str] = [source_list_url, base_url]
        for suffix in ("chapter-directory/", "chapters/", "directory/", "find-a-chapter/", "locations/"):
            seed_urls.append(urljoin(base_url, suffix))

        if self._greedy_collect_mode == _GREEDY_COLLECT_BFS:
            fraternity = _display_name(job.fraternity_slug)
            for query in [
                f'site:{source_host} "{fraternity}" "chapter directory"',
                f'site:{source_host} "{fraternity}" chapters',
            ]:
                for result in self._run_search(query)[:3]:
                    result_host = (urlparse(result.url).netloc or "").lower()
                    if result_host == source_host or result_host.endswith(f".{source_host}"):
                        seed_urls.append(result.url)

        queue: list[tuple[str, int]] = []
        seen_urls: set[str] = set()
        for url in seed_urls:
            normalized = _normalize_url(url)
            if normalized in seen_urls:
                continue
            seen_urls.add(normalized)
            queue.append((url, 0))

        entries: list[NationalsChapterEntry] = []
        visited: set[str] = set()
        page_count = 0
        while queue and page_count < max_pages:
            current_url, depth = queue.pop(0)
            normalized_current = _normalize_url(current_url)
            if normalized_current in visited:
                continue
            visited.add(normalized_current)

            document = self._fetch_search_document(current_url, provider="nationals_directory", query="nationals")
            if document is None:
                continue
            page_count += 1
            entries.extend(_extract_nationals_chapter_entries(document))
            script_seed_links = _extract_nationals_script_seed_urls(document, source_host)

            if depth >= max_depth:
                continue
            for link in [*document.links, *script_seed_links]:
                absolute = urljoin(current_url, link)
                if not _should_follow_nationals_link(absolute, source_host, self._greedy_collect_mode):
                    continue
                normalized_next = _normalize_url(absolute)
                if normalized_next in seen_urls:
                    continue
                seen_urls.add(normalized_next)
                queue.append((absolute, depth + 1))

        deduped: dict[tuple[str, str], NationalsChapterEntry] = {}
        for entry in entries:
            key = (_compact_text(entry.chapter_name), _compact_text(entry.university_name or ""))
            existing = deduped.get(key)
            entry_field_count = sum(1 for value in [entry.website_url, entry.instagram_url, entry.contact_email] if value)
            existing_field_count = sum(1 for value in [existing.website_url, existing.instagram_url, existing.contact_email] if value) if existing else -1
            if existing is None or entry_field_count > existing_field_count or entry.confidence > existing.confidence:
                deduped[key] = entry
        return list(deduped.values())

    def _maybe_ingest_nationals_entries(self, job: FieldJob, entries: list[NationalsChapterEntry]) -> None:
        if self._greedy_collect_mode == _GREEDY_COLLECT_NONE:
            return
        source_slug = job.source_slug or ""
        if not source_slug:
            return
        source_key = f"{source_slug}:{self._greedy_collect_mode}"
        if source_key in self._nationals_collect_attempted:
            return
        self._nationals_collect_attempted.add(source_key)

        source_record = self._load_source_record(source_slug)
        if source_record is None:
            return

        ingest_limit = 40 if self._greedy_collect_mode == _GREEDY_COLLECT_BFS else 12
        upserted = 0
        queued = 0
        for entry in sorted(entries, key=lambda item: item.confidence, reverse=True)[:ingest_limit]:
            if not _nationals_entry_is_ingestible(entry):
                continue
            extracted = ExtractedChapter(
                name=entry.chapter_name,
                university_name=entry.university_name,
                website_url=entry.website_url,
                instagram_url=entry.instagram_url,
                contact_email=entry.contact_email,
                source_url=entry.source_url,
                source_snippet=entry.source_snippet,
                source_confidence=max(0.86, min(entry.confidence, 0.95)),
            )
            try:
                normalized, provenance = normalize_record(source_record, extracted)
            except Exception:
                continue
            if len(normalized.slug) > 120:
                log_event(
                    self._logger,
                    "nationals_greedy_collect_skipped_entry",
                    level=30,
                    chapter_slug=job.chapter_slug,
                    source_slug=job.source_slug,
                    entry_chapter=entry.chapter_name,
                    entry_school=entry.university_name,
                    error="derived_slug_too_long",
                )
                continue
            if len(normalized.name) > 160:
                continue
            if normalized.university_name and len(normalized.university_name) > 180:
                continue

            # Keep greedy ingestion safe: only push positive evidence, never overwrite
            # existing chapter values with nulls or downgrade field states.
            normalized.field_states = _discovered_field_states(normalized)
            try:
                chapter_id = self._repository.upsert_chapter_discovery(source_record, normalized)
                upserted += 1
                if job.crawl_run_id is not None and provenance:
                    self._repository.insert_provenance(chapter_id, source_record.id, job.crawl_run_id, provenance)
                missing_fields = list(normalized.missing_optional_fields)
                if _is_low_signal_university_name(normalized.university_name):
                    missing_fields = [
                        field_name
                        for field_name in missing_fields
                        if field_name not in {FIELD_JOB_FIND_EMAIL, FIELD_JOB_FIND_WEBSITE, FIELD_JOB_FIND_INSTAGRAM}
                    ]
                if job.crawl_run_id is not None and missing_fields:
                    queued += self._repository.create_field_jobs(
                        chapter_id=chapter_id,
                        crawl_run_id=job.crawl_run_id,
                        chapter_slug=normalized.slug,
                        source_slug=source_record.source_slug,
                        missing_fields=missing_fields,
                    )
            except Exception as exc:
                log_event(
                    self._logger,
                    "nationals_greedy_collect_skipped_entry",
                    level=30,
                    chapter_slug=job.chapter_slug,
                    source_slug=job.source_slug,
                    entry_chapter=entry.chapter_name,
                    entry_school=entry.university_name,
                    error=str(exc),
                )
                continue

        if upserted > 0:
            log_event(
                self._logger,
                "nationals_greedy_collect_ingested",
                chapter_slug=job.chapter_slug,
                source_slug=job.source_slug,
                mode=self._greedy_collect_mode,
                upserted=upserted,
                field_jobs_created=queued,
            )

    def _load_source_record(self, source_slug: str) -> SourceRecord | None:
        if source_slug in self._source_record_cache:
            return self._source_record_cache[source_slug]
        sources = self._repository.load_sources(source_slug=source_slug)
        source_record = sources[0] if sources else None
        self._source_record_cache[source_slug] = source_record
        return source_record

    def _extract_email_matches(self, document: SearchDocument, job: FieldJob) -> list[CandidateMatch]:
        if not _document_is_relevant(job, document):
            return []
        matches: list[CandidateMatch] = []
        query = document.query
        for link in document.links:
            email = sanitize_as_email(link)
            if email:
                confidence = self._score_email_candidate(email, document, job, from_mailto=True)
                if not self._email_search_candidate_passes_gate(email, document, job):
                    continue
                matches.append(
                    CandidateMatch(
                        value=email,
                        confidence=confidence,
                        source_url=document.url or link,
                        source_snippet=document.text[:400],
                        field_name="contact_email",
                        source_provider=document.provider,
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for email in _EMAIL_RE.findall(document.text):
            sanitized_email = sanitize_as_email(email)
            if not sanitized_email:
                continue
            confidence = self._score_email_candidate(sanitized_email, document, job, from_mailto=False)
            if not self._email_search_candidate_passes_gate(sanitized_email, document, job):
                continue
            matches.append(
                CandidateMatch(
                    value=sanitized_email,
                    confidence=confidence,
                    source_url=document.url or (_current_website_url(job) or job.source_base_url or "search-enrichment"),
                    source_snippet=document.text[:400],
                    field_name="contact_email",
                    source_provider=document.provider,
                    related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                    query=document.query,
                )
            )
        deobfuscated = _deobfuscate_emails(document.text)
        for email in _EMAIL_RE.findall(deobfuscated):
            sanitized_email = sanitize_as_email(email)
            if not sanitized_email:
                continue
            confidence = self._score_email_candidate(sanitized_email, document, job, from_mailto=False, obfuscated=True)
            if not self._email_search_candidate_passes_gate(sanitized_email, document, job):
                continue
            matches.append(
                CandidateMatch(
                    value=sanitized_email,
                    confidence=confidence,
                    source_url=document.url or (_current_website_url(job) or job.source_base_url or "search-enrichment"),
                    source_snippet=document.text[:400],
                    field_name="contact_email",
                    source_provider=document.provider,
                    related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                    query=document.query,
                )
            )
        return matches

    def _extract_instagram_matches(self, document: SearchDocument, job: FieldJob) -> list[CandidateMatch]:
        if not _instagram_document_is_relevant(job, document):
            return []
        matches: list[CandidateMatch] = []
        query = document.query
        for link in document.links:
            normalized = sanitize_as_instagram(link)
            if normalized:
                if _instagram_looks_institutional_or_directory_account(normalized, document) and not _instagram_handle_has_fraternity_token(normalized, job):
                    self._record_candidate_rejection("instagram", "institutional_account")
                    continue
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=True)
                if not self._instagram_search_candidate_passes_gate(normalized, document, job):
                    continue
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        source_provider=document.provider,
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for match in _INSTAGRAM_RE.findall(document.text):
            normalized = sanitize_as_instagram(match)
            if normalized:
                if _instagram_looks_institutional_or_directory_account(normalized, document) and not _instagram_handle_has_fraternity_token(normalized, job):
                    self._record_candidate_rejection("instagram", "institutional_account")
                    continue
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=True)
                if not self._instagram_search_candidate_passes_gate(normalized, document, job):
                    continue
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        source_provider=document.provider,
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for handle_match in _INSTAGRAM_HANDLE_HINT_RE.finditer(document.text):
            normalized = sanitize_as_instagram(handle_match.group(1))
            if normalized:
                if _instagram_looks_institutional_or_directory_account(normalized, document) and not _instagram_handle_has_fraternity_token(normalized, job):
                    self._record_candidate_rejection("instagram", "institutional_account")
                    continue
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=False)
                if not self._instagram_search_candidate_passes_gate(normalized, document, job):
                    continue
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        source_provider=document.provider,
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for nearby_match in _INSTAGRAM_NEARBY_HANDLE_RE.finditer(document.text):
            normalized = sanitize_as_instagram(nearby_match.group(1))
            if normalized:
                if _instagram_looks_institutional_or_directory_account(normalized, document) and not _instagram_handle_has_fraternity_token(normalized, job):
                    self._record_candidate_rejection("instagram", "institutional_account")
                    continue
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=False)
                if not self._instagram_search_candidate_passes_gate(normalized, document, job):
                    continue
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        source_provider=document.provider,
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        return matches

    def _score_email_candidate(
        self,
        email: str,
        document: SearchDocument,
        job: FieldJob,
        *,
        from_mailto: bool,
        obfuscated: bool = False,
    ) -> float:
        provider_base = {
            "provenance": 0.7,
            "chapter_website": 0.86,
            "nationals_directory": 0.84,
            "search_result": 0.68,
            "search_page": 0.8,
        }.get(document.provider, 0.65)
        confidence = provider_base
        if from_mailto:
            confidence += 0.05
        if obfuscated:
            confidence -= 0.04

        confidence += 0.02 * _score_result_context(job, f"{email} {document.title or ''} {document.text[:200]}")
        confidence += 0.025 * min(6, _email_context_overlap_score(job, email, document))

        local_part = email.split("@", 1)[0].lower()
        domain = _email_domain(email)
        source_tier = _website_trust_tier(job, document.url or "")
        identity_email = _email_local_part_has_identity(email, job)
        generic_office_email = _email_local_part_looks_generic_office(email)
        person_like_email = _email_local_part_looks_personal(email)
        if source_tier == "tier1":
            confidence += 0.05
        if source_tier == "blocked":
            confidence -= 0.25
        lowered_url = (document.url or "").lower()
        if any(marker in lowered_url for marker in ("contact", "officer", "leadership", "board", "about", "staff")):
            confidence += 0.03
        if local_part in _GENERIC_EMAIL_PREFIXES:
            confidence -= 0.08
        if domain.endswith(".edu"):
            confidence += 0.05
        if _email_domain_matches_known_school_or_website(job, domain):
            confidence += 0.04
        if domain in _FREE_EMAIL_DOMAINS and document.provider in {"search_result", "search_page"}:
            confidence -= 0.08
        if document.provider in {"search_result", "search_page"} and not identity_email:
            confidence -= 0.12
        if generic_office_email and document.provider in {"search_result", "search_page"}:
            confidence -= 0.08
        if person_like_email and not identity_email:
            confidence -= 0.16
        if _website_document_looks_low_signal(document):
            confidence -= 0.2
        if _email_document_has_contact_context(document):
            confidence += 0.03

        if not _email_looks_relevant_to_job(email, job, document=document):
            confidence -= 0.24

        return max(0.0, min(0.95, confidence))
    def _score_instagram_candidate(self, instagram_url: str, document: SearchDocument, job: FieldJob, *, direct_url: bool) -> float:
        provider_base = {
            "provenance": 0.8,
            "chapter_website": 0.9,
            "source_page": 0.85,
            "nationals_directory": 0.88,
            "instagram_probe": 0.84,
            "search_result": 0.8,
            "search_page": 0.82,
        }.get(document.provider, 0.68)
        handle_score = _instagram_handle_match_score(instagram_url, job)
        overlap_score = _instagram_context_overlap_score(job, instagram_url, document)
        confidence = provider_base + (0.04 if direct_url else 0.0)
        confidence += 0.02 * _score_result_context(job, f"{instagram_url} {document.title or ''} {document.text[:200]}")
        confidence += 0.03 * min(handle_score, 5)
        confidence += 0.025 * min(overlap_score, 6)
        if document.query and "site:instagram.com" in document.query.lower():
            confidence += 0.03
        if _instagram_has_generic_handle(instagram_url, job):
            confidence -= 0.1
        if not _instagram_looks_relevant_to_job(instagram_url, job, document=document):
            confidence -= 0.24
        return max(0.0, min(0.95, confidence))

    def _search_documents(self, job: FieldJob, target: str, *, include_existing: bool = True) -> list[SearchDocument]:
        documents: list[SearchDocument] = []
        current_website = _current_website_url(job)
        if include_existing:
            source_text = self._source_text(job)
            if source_text:
                documents.append(SearchDocument(text=source_text, provider="provenance", url=job.source_base_url))
            if current_website:
                website_document = self._fetch_search_document(current_website, provider="chapter_website")
                if website_document is not None:
                    documents.append(website_document)

        seen_urls: set[str] = set()
        fetched_pages = 0
        queries = self._build_search_queries(job, target)
        for query in queries:
            query_results = self._run_search(query)

            for result in query_results:
                if not _search_result_is_useful(job, result, target):
                    self._record_candidate_rejection(target, "search_result_not_useful")
                    continue
                if result.url not in seen_urls:
                    documents.append(
                        SearchDocument(
                            text=result.snippet,
                            links=[result.url],
                            url=result.url,
                            title=result.title,
                            provider="search_result",
                            query=query,
                        )
                    )
                seen_urls.add(result.url)
                if fetched_pages >= self._max_search_pages or _should_skip_search_page_fetch(result.url) or not _should_fetch_search_result_page(job, result, target):
                    continue
                fetched = self._fetch_search_document(result.url, provider="search_page", query=query)
                if fetched is not None:
                    documents.append(fetched)
                fetched_pages += 1
        if (
            queries
            and self._search_queries_succeeded == 0
            and self._search_queries_failed >= len(queries)
            and self._search_errors_encountered
        ):
            reason = "provider_unavailable" if self._last_search_failure_kind == "unavailable" else "request_exception"
            self._trace(
                "search_query_fanout_exhausted",
                attempted_queries=self._search_queries_attempted,
                reason=reason,
            )
            log_event(
                self._logger,
                "search_query_fanout_exhausted",
                chapter_slug=job.chapter_slug,
                field_name=job.field_name,
                attempted_queries=self._search_queries_attempted,
                reason=reason,
            )
        return documents

    def _build_search_queries(self, job: FieldJob, target: str) -> list[str]:
        fraternity = _display_name(job.fraternity_slug)
        quoted_fraternity = f'"{fraternity}"' if fraternity else ""
        chapter = job.chapter_name or _display_name(job.chapter_slug)
        university = job.university_name or str(job.payload.get("candidateSchoolName") or "")
        include_chapter = bool(chapter and not _is_generic_greek_letter_chapter_name(chapter))
        campus_domains = _campus_domains(job)

        query_parts: list[list[str]] = []
        if target == "website_school":
            for domain in campus_domains:
                if include_chapter:
                    query_parts.extend(
                        [
                            [quoted_fraternity or fraternity, chapter, university, "student organization", f"site:{domain}"],
                            [quoted_fraternity or fraternity, chapter, university, "greek life", f"site:{domain}"],
                            [quoted_fraternity or fraternity, chapter, university, "chapter profile", f"site:{domain}"],
                        ]
                    )
                query_parts.extend(
                    [
                        [quoted_fraternity or fraternity, university, "student organization", f"site:{domain}"],
                        [quoted_fraternity or fraternity, university, "greek life", f"site:{domain}"],
                        [quoted_fraternity or fraternity, university, "ifc", f"site:{domain}"],
                        [quoted_fraternity or fraternity, university, "chapter profile", f"site:{domain}"],
                        [university, quoted_fraternity or fraternity, "fraternity", f"site:{domain}"],
                    ]
                )
            query_parts.extend(
                [
                    [quoted_fraternity or fraternity, university, "student organization", "site:.edu"],
                    [quoted_fraternity or fraternity, university, "greek life", "site:.edu"],
                    [quoted_fraternity or fraternity, university, "ifc", "site:.edu"],
                    [quoted_fraternity or fraternity, university, "chapter profile", "site:.edu"],
                    [university, quoted_fraternity or fraternity, "fraternity", "site:.edu"],
                    [f'"{fraternity}"' if fraternity else "", f'"{university}"' if university else "", "fraternity", "site:.edu"],
                    [f'"{university}"' if university else "", "student organizations", "site:.edu"],
                    [f'"{university}"' if university else "", "greek life", "site:.edu"],
                    [f'"{university}"' if university else "", "fraternities", "site:.edu"],
                ]
            )
        elif target == "website_fallback":
            if include_chapter:
                query_parts.extend(
                    [
                        [quoted_fraternity or fraternity, chapter, university, "chapter website"],
                        [quoted_fraternity or fraternity, chapter, university, "official chapter site"],
                    ]
                )
            query_parts.extend(
                [
                    [quoted_fraternity or fraternity, university, "chapter website"],
                    [quoted_fraternity or fraternity, university, "official chapter site"],
                    [quoted_fraternity or fraternity, university],
                ]
            )
        elif target == "email":
            website_host = (urlparse(_current_website_url(job) or "").netloc or "").lower()
            if website_host:
                query_parts.extend(
                    [
                        [f"site:{website_host}", quoted_fraternity or fraternity, "contact email"],
                        [f"site:{website_host}", quoted_fraternity or fraternity, "officers email"],
                    ]
                )
            if include_chapter:
                query_parts.extend(
                    [
                        [quoted_fraternity or fraternity, chapter, university, "contact email"],
                        [quoted_fraternity or fraternity, chapter, university, "email"],
                    ]
                )
            query_parts.extend(
                [
                    [quoted_fraternity or fraternity, university, "contact email"],
                    [quoted_fraternity or fraternity, university, "email"],
                    [quoted_fraternity or fraternity, university, "contact", "site:.edu"],
                    [f'"{fraternity}"' if fraternity else "", f'"{university}"' if university else "", "contact email", "site:.edu"],
                ]
            )
        elif target == "affiliation":
            query_parts.extend(
                [
                    [quoted_fraternity or fraternity, university, "fraternity", "site:.edu"],
                    [quoted_fraternity or fraternity, university, "greek life", "site:.edu"],
                    [quoted_fraternity or fraternity, university, "ifc"],
                    [quoted_fraternity or fraternity, university],
                ]
            )
        else:
            fraternity_compact = _compact_text(fraternity)
            handle_queries = _instagram_handle_queries(
                job,
                enable_school_initials=self._enable_school_initials,
                min_school_initial_length=self._min_school_initial_length,
                enable_compact_fraternity=self._enable_compact_fraternity,
            )
            include_chapter_for_instagram = bool(chapter and not _is_generic_greek_letter_chapter_name(chapter))
            query_parts.extend(
                [
                    ["site:instagram.com", f'"{university}"' if university else "", quoted_fraternity or fraternity],
                    [quoted_fraternity or fraternity, university, "instagram"],
                ]
            )
            if self._enable_compact_fraternity and len(fraternity_compact) >= 5:
                query_parts.append(["site:instagram.com", f'"{university}"' if university else "", fraternity_compact])
            if self._instagram_enable_handle_queries:
                for handle_query in handle_queries:
                    query_parts.append(["site:instagram.com", handle_query])
            query_parts.append([quoted_fraternity or fraternity, university, "official instagram"])
            if include_chapter_for_instagram:
                query_parts.append([quoted_fraternity or fraternity, university, chapter, "instagram"])

        fraternity_aliases = _fraternity_query_aliases(fraternity, job.fraternity_slug)
        alias_variant = next((alias for alias in fraternity_aliases if alias and _normalized_match_text(alias) != _normalized_match_text(fraternity)), None)
        if alias_variant:
            if target == "instagram":
                query_parts.extend(
                    [
                        [alias_variant, university, "instagram"],
                        ["site:instagram.com", f'"{university}"' if university else "", alias_variant],
                    ]
                )
            elif target == "email":
                query_parts.append([alias_variant, university, "contact email"])
            elif target == "website_fallback":
                query_parts.append([alias_variant, university, "chapter website"])

        queries = [" ".join(part for part in parts if part).strip() for parts in query_parts]
        if self._search_provider == "bing_html":
            negative_terms = self._bing_negative_terms(job)
            if negative_terms:
                suffix = " ".join(negative_terms)
                queries = [f"{query} {suffix}".strip() for query in queries]
        deduped = list(dict.fromkeys(query for query in queries if query))
        if target == "email":
            return deduped[: self._email_max_queries]
        if target == "instagram":
            return deduped[: self._instagram_max_queries]
        return deduped

    def _run_search(self, query: str) -> list[SearchResult]:
        if self._search_client is None:
            return []

        cached_results = self._search_result_cache.get(query)
        if cached_results is not None:
            self._last_query_provider_attempts = []
            self._last_search_failure_kind = None
            self._trace("search_cache_hit", query=query, result_count=len(cached_results))
            log_event(self._logger, "search_query_cache_hit", query=query, result_count=len(cached_results))
            return list(cached_results)

        self._search_queries_attempted += 1
        self._last_query_provider_attempts = []
        self._last_search_failure_kind = None
        try:
            results = self._search_client.search(query)
            provider_attempts = self._consume_provider_attempts()
            self._last_query_provider_attempts = provider_attempts
            self._last_provider_attempts.extend(provider_attempts)
        except SearchUnavailableError as exc:
            self._search_errors_encountered = True
            self._search_queries_failed += 1
            provider_attempts = self._consume_provider_attempts()
            self._last_query_provider_attempts = provider_attempts
            self._last_search_failure_kind = "unavailable"
            self._last_provider_attempts.extend(provider_attempts)
            self._trace("search_failed", query=query, error_type="unavailable")
            log_event(
                self._logger,
                "search_unavailable",
                level=30,
                query=query,
                error=str(exc),
                provider_attempts=provider_attempts,
            )
            return []
        except requests.RequestException as exc:
            self._search_errors_encountered = True
            self._search_queries_failed += 1
            provider_attempts = self._consume_provider_attempts()
            self._last_query_provider_attempts = provider_attempts
            self._last_search_failure_kind = "request_exception"
            self._last_provider_attempts.extend(provider_attempts)
            self._trace("search_failed", query=query, error_type="request_exception")
            log_event(
                self._logger,
                "search_request_failed",
                level=30,
                query=query,
                error=str(exc),
                provider_attempts=provider_attempts,
            )
            return []

        self._search_queries_succeeded += 1
        self._last_search_failure_kind = None
        if results or self._cache_empty_search_results:
            self._search_result_cache[query] = list(results)
        self._trace("search_executed", query=query, result_count=len(results))
        log_event(
            self._logger,
            "search_query_executed",
            query=query,
            result_count=len(results),
            provider_attempts=provider_attempts,
        )
        return results

    def _fetch_search_document(self, url: str, provider: str, query: str | None = None) -> SearchDocument | None:
        cache_key = _normalize_url(url)
        if cache_key in self._search_document_cache:
            cached_document = self._search_document_cache[cache_key]
            if cached_document is None:
                return None
            return SearchDocument(
                text=cached_document.text,
                links=list(cached_document.links),
                url=url,
                title=cached_document.title,
                provider=provider,
                query=query,
                html=cached_document.html,
            )

        try:
            response = self._get_requester(url, timeout=10, allow_redirects=True)
        except requests.RequestException:
            self._search_document_cache[cache_key] = None
            return None

        status_code = getattr(response, "status_code", None)
        if status_code is not None and status_code >= 400:
            self._search_document_cache[cache_key] = None
            return None

        html = getattr(response, "text", "") or ""
        if not html:
            self._search_document_cache[cache_key] = None
            return None

        soup = BeautifulSoup(html, "html.parser")
        links = [href.strip() for href in (node.get("href") for node in soup.select("a[href]")) if href and href.strip()]
        text = " ".join(soup.stripped_strings)
        title = soup.title.get_text(" ", strip=True) if soup.title else None
        self._search_document_cache[cache_key] = SearchDocument(
            text=text,
            links=list(links),
            url=url,
            title=title,
            provider="cached",
            html=html,
        )
        return SearchDocument(text=text, links=links, url=url, title=title, provider=provider, query=query, html=html)

    def _already_populated_result(self, field_name: str, value: str) -> FieldJobResult:
        state_key = FIELD_JOB_TO_STATE_KEY[field_name]
        return FieldJobResult(
            chapter_updates={},
            completed_payload={"status": "already_populated", "value": value, "decision_trace": self._build_decision_trace_summary()},
            field_state_updates={state_key: "found"},
        )

    def _source_text(self, job: FieldJob) -> str:
        cached = self._provenance_text_cache.get(job.chapter_id)
        if cached is not None:
            return cached
        snippets = self._repository.fetch_provenance_snippets(job.chapter_id)
        source_text = "\n".join(snippets)
        self._provenance_text_cache[job.chapter_id] = source_text
        return source_text

    def _requires_website_first(self, job: FieldJob) -> bool:
        if job.field_name != FIELD_JOB_FIND_EMAIL:
            return False
        if not self._require_confident_website_for_email:
            return False
        if not self._repository.has_pending_field_job(job.chapter_id, FIELD_JOB_FIND_WEBSITE):
            return False
        if self._email_escape_on_provider_block and self._repository.has_recent_transient_website_failures(
            job.chapter_id,
            min_failures=self._email_escape_min_website_failures,
        ):
            self._trace(
                "dependency_escaped",
                reason="website_provider_blocked",
                min_failures=self._email_escape_min_website_failures,
            )
            return False
        return True

    def _write_threshold(self, job: FieldJob, target_field: str, match: CandidateMatch) -> float:
        if target_field == "website_url":
            candidate_tier = _website_trust_tier(job, match.value)
            source_tier = _website_trust_tier(job, match.source_url)
            if candidate_tier == "tier2":
                return 1.0
            if match.source_provider in {"search_result", "search_page"}:
                if candidate_tier == "tier1" or source_tier == "tier1":
                    return 0.90
                if match.source_provider == "search_result":
                    return 0.98
                return 0.95
        if match.source_provider in {"search_result", "search_page"}:
            return {
                "website_url": 0.95,
                "contact_email": 0.90,
                "instagram_url": 0.88,
            }.get(target_field, 0.85)
        return 0.65

    def _found_threshold(self, job: FieldJob, target_field: str, match: CandidateMatch) -> float:
        if target_field == "website_url":
            candidate_tier = _website_trust_tier(job, match.value)
            source_tier = _website_trust_tier(job, match.source_url)
            if candidate_tier == "tier2":
                return 0.99
            if match.source_provider in {"search_result", "search_page"} and (candidate_tier == "tier1" or source_tier == "tier1"):
                return 0.92
        if match.source_provider in {"search_result", "search_page"}:
            return {
                "website_url": 0.96,
                "contact_email": 0.92,
                "instagram_url": 0.90,
            }.get(target_field, 0.90)
        return 0.85

    def _no_candidate_error(self, job: FieldJob, message: str) -> RetryableJobError:
        if self._search_errors_encountered:
            all_queries_failed = self._search_queries_attempted > 0 and self._search_queries_failed >= self._search_queries_attempted
            if all_queries_failed:
                previous_transient_failures = self._payload_int(job.payload.get("transient_provider_failures"))
                next_transient_failures = previous_transient_failures + 1
                if next_transient_failures > self._transient_short_retries:
                    cooldown_seconds = max(
                        self._transient_long_cooldown_seconds,
                        self._dependency_wait_seconds,
                        self._base_backoff_seconds,
                    )
                    return RetryableJobError(
                        f"{message}; search provider or network unavailable",
                        backoff_seconds=cooldown_seconds,
                        preserve_attempt=True,
                        reason_code="transient_network",
                    )
                return RetryableJobError(
                    f"{message}; search provider or network unavailable",
                    backoff_seconds=max(self._dependency_wait_seconds, self._base_backoff_seconds),
                    preserve_attempt=True,
                    reason_code="transient_network",
                )
            return RetryableJobError(message, backoff_seconds=self._base_backoff_seconds, reason_code="provider_low_signal")

        low_signal = self._search_provider == "bing_html" and job.field_name == FIELD_JOB_FIND_WEBSITE
        if low_signal:
            # Keep low-signal website jobs delayed, but consume attempts so they cannot
            # loop forever when query quality is poor.
            return RetryableJobError(
                message,
                backoff_seconds=max(
                    self._negative_result_cooldown_seconds,
                    self._base_backoff_seconds,
                    self._min_no_candidate_backoff_seconds,
                ),
                low_signal=True,
                preserve_attempt=False,
                reason_code="provider_low_signal",
            )
        return RetryableJobError(
            message,
            backoff_seconds=max(self._negative_result_cooldown_seconds, self._min_no_candidate_backoff_seconds),
            low_signal=False,
            reason_code="terminal_no_candidate",
        )

    def _record_candidate_rejection(self, target: str, reason: str) -> None:
        key = f"{target}:{reason}"
        self._candidate_rejection_counts[key] = self._candidate_rejection_counts.get(key, 0) + 1

    def _trace(self, event: str, **payload: str | int | float | bool | None) -> None:
        entry: dict[str, str | int | float | bool | None] = {"event": event}
        entry.update(payload)
        self._decision_trace.append(entry)

    def _consume_provider_attempts(self) -> list[dict[str, object]]:
        if self._search_client is None:
            return []
        consume = getattr(self._search_client, "consume_last_provider_attempts", None)
        if not callable(consume):
            return []
        attempts = consume()
        return attempts if isinstance(attempts, list) else []

    def _build_requeue_payload_patch(self, job: FieldJob, exc: "RetryableJobError", backoff_seconds: int) -> dict[str, object]:
        patch: dict[str, object] = {}
        queue_state = "deferred" if exc.reason_code in {"transient_network", "dependency_wait", "provider_low_signal"} else "actionable"
        patch["contactResolution"] = {
            "queueState": queue_state,
            "reasonCode": exc.reason_code,
            "nextBackoffSeconds": backoff_seconds,
        }
        if exc.reason_code == "transient_network":
            previous_transient_failures = self._payload_int(job.payload.get("transient_provider_failures"))
            patch["transient_provider_failures"] = previous_transient_failures + 1
            patch["transient_provider_last_error"] = str(exc)
            patch["transient_provider_last_reason"] = exc.reason_code
            patch["transient_provider_last_backoff_seconds"] = backoff_seconds
        if self._last_provider_attempts:
            patch["provider_attempts"] = self._last_provider_attempts[-8:]
        if exc.reason_code in {"terminal_no_candidate", "provider_low_signal"}:
            patch["terminal_no_signal_count"] = self._payload_int(job.payload.get("terminal_no_signal_count")) + 1
        return patch

    def _should_abort_search_fanout(self, query_results: list[SearchResult]) -> bool:
        # Fanout abort is disabled so field jobs can exhaust bounded query budgets
        # before classifying provider outages.
        return False

    def _payload_int(self, raw_value: object) -> int:
        try:
            return int(raw_value or 0)
        except (TypeError, ValueError):
            return 0

    def _build_decision_trace_summary(self) -> dict[str, object]:
        rejection_histogram = [
            {"reason": reason, "count": count}
            for reason, count in sorted(self._candidate_rejection_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        ]
        return {
            "trace": self._decision_trace[-30:],
            "search": {
                "attempted": self._search_queries_attempted,
                "succeeded": self._search_queries_succeeded,
                "failed": self._search_queries_failed,
                "fanoutAborted": self._search_fanout_aborted,
                "providerAttempts": self._last_provider_attempts[-8:],
            },
            "rejections": rejection_histogram,
        }

    def _candidate_rejection_summary_payload(self) -> dict[str, object] | None:
        if not self._candidate_rejection_counts:
            return None
        top_reasons = sorted(self._candidate_rejection_counts.items(), key=lambda item: item[1], reverse=True)[:8]
        return {
            "totalRejections": sum(self._candidate_rejection_counts.values()),
            "uniqueReasons": len(self._candidate_rejection_counts),
            "topReasons": [{"reason": reason, "count": count} for reason, count in top_reasons],
        }

    def _emit_candidate_rejection_summary(self, job: FieldJob, *, target: str) -> None:
        if not self._candidate_rejection_counts:
            return
        total = sum(self._candidate_rejection_counts.values())
        top_reasons = sorted(self._candidate_rejection_counts.items(), key=lambda item: item[1], reverse=True)[:8]
        log_event(
            self._logger,
            "candidate_rejection_summary",
            chapter_slug=job.chapter_slug,
            field_name=job.field_name,
            target=target,
            total_rejections=total,
            unique_reasons=len(self._candidate_rejection_counts),
            top_reasons=[{"reason": reason, "count": count} for reason, count in top_reasons],
            search_queries_attempted=self._search_queries_attempted,
            search_queries_succeeded=self._search_queries_succeeded,
            search_queries_failed=self._search_queries_failed,
        )

    def _detect_search_provider(self, search_client: SearchClient | None) -> str:
        settings = getattr(search_client, "_settings", None)
        provider = getattr(settings, "crawler_search_provider", None)
        return str(provider or "auto")

    def _retry_limit(self, job: FieldJob, exc: "RetryableJobError") -> int:
        if exc.low_signal and job.field_name == FIELD_JOB_FIND_WEBSITE:
            return min(job.max_attempts, 2)
        return job.max_attempts

    def _email_search_candidate_passes_gate(self, email: str, document: SearchDocument, job: FieldJob) -> bool:
        if document.provider not in {"search_result", "search_page"}:
            return True

        combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or "", email] if part))
        school_match = _school_matches(job, combined)
        fraternity_match = _fraternity_matches(job, combined)
        chapter_match = _chapter_matches(job, combined)
        overlap_score = _email_context_overlap_score(job, email, document)
        email_domain = _email_domain(email)
        source_tier = _website_trust_tier(job, document.url or "")
        identity_email = _email_local_part_has_identity(email, job)
        person_like_email = _email_local_part_looks_personal(email)
        generic_office_email = _email_local_part_looks_generic_office(email)
        strong_context = _email_document_has_contact_context(document) and fraternity_match and (school_match or chapter_match)
        official_chapter_context = _website_document_has_official_chapter_context(job, document)

        if _website_document_looks_low_signal(document):
            self._record_candidate_rejection("email", "low_signal_page")
            return False
        if generic_office_email and not identity_email and not strong_context:
            self._record_candidate_rejection("email", "generic_office_email")
            return False

        if fraternity_match and (school_match or chapter_match):
            return True
        if source_tier == "tier1" and official_chapter_context and school_match and (fraternity_match or chapter_match):
            return True
        if source_tier == "tier1" and official_chapter_context and school_match and _email_domain_matches_known_school_or_website(job, email_domain) and (identity_email or strong_context):
            return True
        if overlap_score >= 4 and (school_match or fraternity_match):
            return True
        if _email_domain_matches_known_school_or_website(job, email_domain) and (school_match or fraternity_match):
            return True
        if person_like_email and not strong_context:
            self._record_candidate_rejection("email", "person_like_email")
            return False
        if source_tier == "tier1" and not identity_email and not strong_context and not official_chapter_context:
            self._record_candidate_rejection("email", "missing_identity_anchor")
            return False
        if not school_match:
            self._record_candidate_rejection("email", "missing_school_anchor")
        if not fraternity_match and not chapter_match:
            self._record_candidate_rejection("email", "missing_fraternity_anchor")
        if overlap_score < 4:
            self._record_candidate_rejection("email", "low_context_overlap")
        if source_tier not in {"tier1", "unknown"}:
            self._record_candidate_rejection("email", "low_trust_source_tier")
        return False

    def _instagram_search_candidate_passes_gate(self, instagram_url: str, document: SearchDocument, job: FieldJob) -> bool:
        if document.provider not in {"search_result", "search_page"}:
            return True
        combined = _instagram_candidate_text(document, instagram_url)
        handle_score = _instagram_handle_match_score(instagram_url, job)
        overlap_score = _instagram_context_overlap_score(job, instagram_url, document)
        school_match = _school_matches(job, combined)
        fraternity_match = _fraternity_matches(job, combined)
        chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
        if _instagram_has_conflicting_org_signal(job, combined) and handle_score < 5:
            self._record_candidate_rejection("instagram", "conflicting_org_signal")
            return False
        if _chapter_designation_signal(job, combined) < 0:
            self._record_candidate_rejection("instagram", "chapter_designation_mismatch")
            return False
        if not _instagram_handle_has_fraternity_token(instagram_url, job) and _chapter_designation_signal(job, combined) <= 0:
            self._record_candidate_rejection("instagram", "missing_handle_fraternity_token")
            return False
        if handle_score >= 4 and (school_match or fraternity_match):
            return True
        if handle_score >= 2 and fraternity_match and (school_match or chapter_match):
            return True
        if handle_score >= 3 and school_match and fraternity_match:
            return True
        if overlap_score >= 4 and school_match and fraternity_match and handle_score >= 1:
            return True
        if overlap_score >= 5 and school_match and (fraternity_match or chapter_match) and (handle_score >= 1 or chapter_match):
            return True
        if _chapter_designation_signal(job, combined) > 0 and handle_score >= 2 and (school_match or fraternity_match):
            return True
        if not school_match:
            self._record_candidate_rejection("instagram", "missing_school_anchor")
        if not fraternity_match and not chapter_match:
            self._record_candidate_rejection("instagram", "missing_fraternity_anchor")
        if handle_score < 3:
            self._record_candidate_rejection("instagram", "weak_handle_match")
        if overlap_score < 4:
            self._record_candidate_rejection("instagram", "low_context_overlap")
        return False

    def _bing_negative_terms(self, job: FieldJob) -> list[str]:
        fraternity = _normalized_match_text(_display_name(job.fraternity_slug))
        if "sigma" not in fraternity.split():
            return []
        return ['-"sigma aldrich"', '-sigmaaldrich', '-millipore', '-merck']

    def _score_website_candidate(self, website_url: str, document: SearchDocument, job: FieldJob) -> float:
        candidate_tier = _website_trust_tier(job, website_url)
        if candidate_tier == "blocked":
            return 0.0
        provider_base = {
            "provenance": 0.72,
            "search_result": 0.82,
            "search_page": 0.86,
            "chapter_website": 0.9,
            "nationals_directory": 0.88,
        }.get(document.provider, 0.68)
        confidence = provider_base
        lowered = document.text.lower()
        if "website" in lowered or "official" in lowered:
            confidence += 0.08
        document_host = (urlparse(document.url or "").netloc or "").lower()
        candidate_host = (urlparse(website_url).netloc or "").lower()
        document_tier = _website_trust_tier(job, document.url or "")
        if candidate_tier == "tier1":
            confidence += 0.08
        if document_tier == "tier1":
            confidence += 0.08
        if document.provider == "search_page" and document_host and candidate_host and document_host != candidate_host:
            confidence += 0.05
        if document_tier == "tier1" and document.provider == "search_page" and _looks_like_directory_listing_url(document.url or ""):
            confidence += 0.08
            if candidate_host and candidate_host != document_host and not _looks_like_directory_listing_url(website_url):
                confidence += 0.08
        if _looks_like_directory_listing_url(website_url):
            confidence -= 0.18
            if document.provider == "search_result":
                confidence -= 0.12
        if candidate_tier == "tier2":
            confidence = min(confidence - 0.08, 0.78)
        confidence += 0.03 * _score_result_context(job, f"{website_url} {document.title or ''} {document.text[:200]}")
        return max(0.0, min(0.95, confidence))

    def _score_website_result(self, job: FieldJob, result: SearchResult) -> float:
        if _is_disallowed_website_candidate(result.url):
            return 0.0
        confidence = 0.78
        confidence += 0.03 * _score_result_context(job, f"{result.title} {result.snippet} {result.url}")
        hostname = (urlparse(result.url).netloc or "").lower()
        lowered = f"{result.title} {result.snippet}".lower()
        if "official" in lowered or "chapter" in lowered:
            confidence += 0.04
        if ".edu" in hostname:
            confidence += 0.05
        if any(keyword in lowered for keyword in ("student organization", "greek life", "fraternity")):
            confidence += 0.03
        if _looks_like_directory_listing_url(result.url):
            confidence -= 0.15
        return max(0.0, min(0.95, confidence))

    def _extract_website_matches(self, document: SearchDocument, job: FieldJob) -> list[CandidateMatch]:
        if (
            document.provider != "provenance"
            and not _document_is_relevant(job, document)
            and not _website_document_passes_relaxed_gate(job, document)
        ):
            self._record_candidate_rejection("website", "document_not_relevant")
            return []
        matches: list[CandidateMatch] = []
        candidates = list(document.links)
        candidates.extend(_URL_RE.findall(document.text))
        seen: set[str] = set()
        for candidate in candidates:
            raw_url = candidate.strip().rstrip('.,;)')
            url = sanitize_as_website(raw_url, base_url=document.url or job.source_base_url)
            if not url:
                if raw_url.lower().startswith("mailto:"):
                    self._record_candidate_rejection("website", "kind_mismatch_mailto")
                continue
            if _is_disallowed_website_candidate(url):
                self._record_candidate_rejection("website", "blocked_host")
                continue
            if job.source_base_url and _normalize_url(url) == _normalize_url(job.source_base_url):
                self._record_candidate_rejection("website", "source_base_url_only")
                continue
            if _looks_like_document_asset_url(url):
                self._record_candidate_rejection("website", "document_asset")
                continue
            if _website_candidate_looks_low_signal(url, document):
                self._record_candidate_rejection("website", "low_signal_url")
                continue
            key = _normalize_url(url)
            if key in seen:
                continue
            seen.add(key)
            if _candidate_is_source_domain(url, job):
                self._record_candidate_rejection("website", "source_domain_url")
                continue
            if _website_document_has_conflicting_org_signal(job, document):
                self._record_candidate_rejection("website", "conflicting_org_signal")
                continue
            if _school_has_conflicting_signal(job, _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or "", url] if part))):
                self._record_candidate_rejection("website", "conflicting_school_signal")
                continue
            document_host = (urlparse(document.url or "").netloc or "").lower()
            candidate_host = (urlparse(url).netloc or "").lower()
            if (
                document.provider == "search_page"
                and _website_trust_tier(job, document.url or "") == "tier1"
                and candidate_host
                and document_host
                and candidate_host != document_host
                and not _trusted_directory_external_candidate(job, url, document)
                and not _search_page_link_has_website_context(job, document, url)
            ):
                self._record_candidate_rejection("website", "weak_external_link_context")
                continue
            if document.provider in {"search_result", "search_page"} and _website_trust_tier(job, url) == "tier1":
                if _looks_like_generic_site_root(url):
                    self._record_candidate_rejection("website", "generic_school_root")
                    continue
                if not _ambiguous_school_tier1_candidate_allowed(job, url, document):
                    self._record_candidate_rejection("website", "ambiguous_school_tier1_generic")
                    continue
                if not _tier1_website_candidate_has_specificity(job, url, document):
                    self._record_candidate_rejection("website", "low_specificity_tier1")
                    continue
            confidence = self._score_website_candidate(url, document, job)
            if _trusted_directory_external_candidate(job, url, document):
                confidence = min(0.95, confidence + 0.08)
            if confidence < 0.65:
                self._record_candidate_rejection("website", "confidence_below_floor")
                continue
            matches.append(
                CandidateMatch(
                    value=url,
                    confidence=confidence,
                    source_url=document.url or url,
                    source_snippet=document.text[:400],
                    field_name="website_url",
                    source_provider=document.provider,
                    query=document.query,
                )
            )
        return matches

def _deobfuscate_emails(value: str) -> str:
    text = value
    text = _OBFUSCATED_AT_RE.sub("@", text)
    text = _OBFUSCATED_DOT_RE.sub(".", text)
    return text



def _normalize_instagram_candidate(value: str | None) -> str | None:
    return sanitize_as_instagram(value)


def _instagram_profile_looks_missing(document: SearchDocument) -> bool:
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:2400], document.url or ""] if part))
    return any(
        marker in combined
        for marker in (
            "sorry this page isn t available",
            "the link you followed may be broken",
            "page isn t available",
            "page not found",
            "user not found",
        )
    )


def _is_generic_greek_letter_chapter_name(value: str | None) -> bool:
    tokens = _normalized_match_text(value).split()
    return bool(tokens) and len(tokens) <= 4 and all(token in _GREEK_LETTER_TOKENS for token in tokens)


def _normalized_match_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _compact_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _current_website_url(job: FieldJob) -> str | None:
    return sanitize_as_website(job.website_url, base_url=job.source_base_url)


def _source_list_url_for_job(job: FieldJob, source_record: SourceRecord | None = None) -> str:
    payload_list_url = str(job.payload.get("sourceListUrl") or "").strip()
    if payload_list_url:
        return payload_list_url
    if source_record is not None:
        list_url = getattr(source_record, "list_url", None)
        if isinstance(list_url, str) and list_url.strip():
            return list_url.strip()
        list_path = getattr(source_record, "list_path", None)
        base_url = str(getattr(source_record, "base_url", "") or "").strip()
        if isinstance(list_path, str) and list_path.strip():
            if list_path.startswith("http"):
                return list_path.strip()
            if base_url:
                return urljoin(base_url, list_path)
    return str(job.source_base_url or "").strip()


def _initialism(value: str | None) -> str:
    tokens = [token for token in _normalized_match_text(value).split() if token not in {"of", "the", "and", "at"}]
    return "".join(token[0] for token in tokens)


def _school_initials(value: str | None) -> str:
    return _initialism(value)


def _school_aliases(
    value: str | None,
    *,
    enable_school_initials: bool = True,
    min_school_initial_length: int = 3,
) -> list[str]:
    aliases: list[str] = []
    initials = _school_initials(value)
    if enable_school_initials and len(initials) >= min_school_initial_length:
        aliases.append(initials)
    return list(dict.fromkeys(alias.lower() for alias in aliases if alias))


def _school_query_aliases(
    value: str | None,
    *,
    enable_school_initials: bool = True,
    min_school_initial_length: int = 3,
) -> list[str]:
    normalized = " ".join((value or "").split()).strip()
    aliases: list[str] = []
    if normalized:
        aliases.append(normalized)
    compact = _compact_text(normalized)
    if len(compact) >= 6:
        aliases.append(compact)
    aliases.extend(
        _school_aliases(
            normalized,
            enable_school_initials=enable_school_initials,
            min_school_initial_length=min_school_initial_length,
        )
    )
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _fraternity_query_aliases(display_name: str | None, fraternity_slug: str | None) -> list[str]:
    display = " ".join((display_name or "").split()).strip()
    aliases: list[str] = []
    if display:
        aliases.append(display)
    compact = _compact_text(display)
    if len(compact) >= 6:
        aliases.append(compact)
    initials = _initialism(display)
    if len(initials) >= 2:
        aliases.append(initials)
    slug_tokens = " ".join(token for token in _normalized_match_text(fraternity_slug).split() if token not in {"main", "national"})
    if slug_tokens and slug_tokens not in [token.lower() for token in aliases]:
        aliases.append(slug_tokens)
    alias_map = {
        "phi gamma delta": ["fiji"],
        "alpha tau omega": ["ato"],
        "sigma alpha epsilon": ["sae"],
        "beta upsilon chi": ["byx"],
        "pi kappa alpha": ["pike"],
    }
    canonical = _normalized_match_text(display)
    for mapped_alias in alias_map.get(canonical, []):
        aliases.append(mapped_alias)
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _instagram_handle_queries(
    job: FieldJob,
    *,
    enable_school_initials: bool = True,
    min_school_initial_length: int = 3,
    enable_compact_fraternity: bool = True,
) -> list[str]:
    fraternity = _display_name(job.fraternity_slug)
    fraternity_compact = _compact_text(fraternity) if enable_compact_fraternity else ""
    if len(fraternity_compact) < 5:
        return []
    school_aliases = _school_aliases(
        job.university_name or str(job.payload.get("candidateSchoolName") or ""),
        enable_school_initials=enable_school_initials,
        min_school_initial_length=min_school_initial_length,
    )
    candidates: list[str] = []
    for school_alias in school_aliases[:1]:
        candidates.extend(
            [
                f"{school_alias}{fraternity_compact}",
                f"{fraternity_compact}{school_alias}",
                f"{school_alias}_{fraternity_compact}",
                f"{fraternity_compact}_{school_alias}",
            ]
        )
    return list(dict.fromkeys(candidate for candidate in candidates if len(candidate) >= 6))[:4]


def _school_handle_aliases(
    value: str | None,
    *,
    enable_school_initials: bool = True,
    min_school_initial_length: int = 3,
) -> list[str]:
    normalized = " ".join((value or "").split()).strip()
    aliases: list[str] = []
    aliases.extend(
        _school_aliases(
            normalized,
            enable_school_initials=enable_school_initials,
            min_school_initial_length=min_school_initial_length,
        )
    )
    for token in _significant_tokens(normalized):
        compact = _compact_text(token)
        if len(compact) >= 4:
            aliases.append(compact)
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _instagram_probe_handles(
    job: FieldJob,
    *,
    enable_school_initials: bool = True,
    min_school_initial_length: int = 3,
    enable_compact_fraternity: bool = True,
    max_candidates: int = 6,
) -> list[str]:
    fraternity_display = _display_name(job.fraternity_slug)
    fraternity_aliases = _fraternity_query_aliases(fraternity_display, job.fraternity_slug)
    fraternity_compacts = [
        _compact_text(alias)
        for alias in fraternity_aliases
        if len(_compact_text(alias)) >= 3
    ]
    fraternity_compacts = list(dict.fromkeys(fraternity_compacts))
    if enable_compact_fraternity:
        base_compact = _compact_text(fraternity_display)
        if len(base_compact) >= 5 and base_compact not in fraternity_compacts:
            fraternity_compacts.insert(0, base_compact)

    school = job.university_name or str(job.payload.get("candidateSchoolName") or "")
    school_aliases = _school_handle_aliases(
        school,
        enable_school_initials=enable_school_initials,
        min_school_initial_length=min_school_initial_length,
    )

    handles: list[str] = []
    for school_alias in school_aliases[:3]:
        compact_school = _compact_text(school_alias)
        if not compact_school:
            continue
        for fraternity_compact in fraternity_compacts[:4]:
            if not fraternity_compact:
                continue
            handles.extend(
                [
                    f"{compact_school}{fraternity_compact}",
                    f"{fraternity_compact}{compact_school}",
                    f"{compact_school}_{fraternity_compact}",
                    f"{fraternity_compact}_{compact_school}",
                ]
            )
    return list(dict.fromkeys(handle for handle in handles if 6 <= len(handle) <= 30))[: max(1, max_candidates)]


def _email_followup_links(document: SearchDocument, website_url: str, *, limit: int) -> list[str]:
    if limit <= 0:
        return []

    base_host = (urlparse(website_url).netloc or "").lower()
    if not base_host:
        return []

    hints: tuple[tuple[str, int], ...] = (
        ("contact", 4),
        ("officer", 3),
        ("leadership", 3),
        ("executive", 3),
        ("board", 2),
        ("about", 2),
        ("join", 1),
        ("recruit", 1),
        ("rush", 1),
    )

    scored: list[tuple[int, str]] = []
    for href in document.links:
        if not href or href.lower().startswith("mailto:"):
            continue
        absolute = urljoin(website_url, href)
        parsed = urlparse(absolute)
        host = (parsed.netloc or "").lower()
        if not host or host != base_host:
            continue
        if parsed.scheme not in {"http", "https"}:
            continue
        normalized = _normalize_url(absolute)
        lowered = normalized.lower()
        score = sum(weight for marker, weight in hints if marker in lowered)
        if score <= 0:
            continue
        scored.append((score, absolute))

    deduped: list[str] = []
    seen: set[str] = set()
    for _, url in sorted(scored, key=lambda item: (-item[0], item[1])):
        key = _normalize_url(url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(url)
        if len(deduped) >= limit:
            break
    return deduped

def _significant_tokens(value: str | None) -> list[str]:
    tokens = [token for token in _normalized_match_text(value).split() if len(token) >= 4 and token not in _MATCH_STOPWORDS]
    if tokens:
        return tokens
    return [token for token in _normalized_match_text(value).split() if len(token) >= 3 and token not in {"the", "and", "of", "for"}]


def _canonical_fraternity_display(value: str | None) -> str:
    tokens = [token for token in _normalized_match_text(_display_name(value)).split() if token not in _FRATERNITY_NON_IDENTITY_TOKENS]
    if tokens:
        return " ".join(tokens)
    return _normalized_match_text(_display_name(value))


def _fraternity_tokens(value: str | None) -> list[str]:
    return [
        token
        for token in _normalized_match_text(_canonical_fraternity_display(value)).split()
        if len(token) >= 3 and token not in {"the", "and", "of", "for"}
    ]


def _fraternity_matches(job: FieldJob, text: str) -> bool:
    fraternity_display = _canonical_fraternity_display(job.fraternity_slug)
    fraternity_phrase = _normalized_match_text(fraternity_display)
    if not fraternity_phrase or not text:
        return False
    compact_text = _compact_text(text)
    fraternity_compact = _compact_text(fraternity_display)
    if fraternity_phrase and fraternity_phrase in text:
        return True
    if fraternity_compact and fraternity_compact in compact_text:
        return True
    for alias in _fraternity_query_aliases(fraternity_display, job.fraternity_slug):
        alias_phrase = _normalized_match_text(alias)
        alias_compact = _compact_text(alias)
        if alias_phrase and alias_phrase in text:
            return True
        if alias_compact and len(alias_compact) <= 3 and alias_compact.isalpha():
            if re.search(rf"\b{re.escape(alias_compact)}\b", text):
                return True
            continue
        if alias_compact and alias_compact in compact_text:
            return True
    tokens = _fraternity_tokens(fraternity_display)
    if not tokens:
        return False
    if all(token in _GREEK_LETTER_TOKENS for token in tokens):
        return all(token in text for token in tokens)
    required = len(tokens) if len(tokens) <= 2 else 2
    return sum(1 for token in tokens if token in text) >= required


def _school_has_conflicting_signal(job: FieldJob, text: str) -> bool:
    school = _normalized_match_text(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    if not school or not text:
        return False
    if school.startswith("university of "):
        core = school.removeprefix("university of ").strip()
        if core and (f"{core} state" in text or f"{core} state university" in text):
            return True
    if school.endswith(" state university"):
        core = school.removesuffix(" state university").strip()
        if core and f"university of {core}" in text and school not in text:
            return True
    return False


def _school_identity_tokens(job: FieldJob) -> list[str]:
    university = job.university_name or str(job.payload.get("candidateSchoolName") or "")
    return _significant_tokens(university)


def _school_name_is_ambiguous(job: FieldJob) -> bool:
    return len(_school_identity_tokens(job)) <= 1


def _school_matches(job: FieldJob, text: str) -> bool:
    university = job.university_name or str(job.payload.get("candidateSchoolName") or "")
    phrase = _normalized_match_text(university)
    if phrase and phrase in text:
        return True
    tokens = _significant_tokens(university)
    if not tokens:
        return False
    required = 2 if len(tokens) >= 2 else 1
    matched = sum(1 for token in tokens if token in text)
    return matched >= required


def _chapter_signal_tokens(job: FieldJob) -> list[str]:
    school_tokens = set(_significant_tokens(job.university_name or str(job.payload.get("candidateSchoolName") or "")))
    fraternity_tokens = set(_fraternity_tokens(_display_name(job.fraternity_slug)))
    chapter_tokens = [
        token
        for token in _significant_tokens(job.chapter_name)
        if token not in school_tokens and token not in fraternity_tokens and token not in _CHAPTER_SIGNAL_STOPWORDS
    ]
    if chapter_tokens:
        return chapter_tokens
    slug_tokens = [
        token
        for token in _significant_tokens(job.chapter_slug)
        if token not in school_tokens and token not in fraternity_tokens and token not in _CHAPTER_SIGNAL_STOPWORDS
    ]
    return slug_tokens


def _chapter_matches(job: FieldJob, text: str) -> bool:
    chapter_tokens = _chapter_signal_tokens(job)
    return sum(1 for token in chapter_tokens if token in text) >= 1


def _url_has_job_identity(job: FieldJob, url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(url if "://" in url else f"https://{url}")
    combined = _normalized_match_text(" ".join(part for part in [parsed.netloc or "", parsed.path or "", parsed.query or ""] if part))
    if not combined:
        return False
    if _fraternity_matches(job, combined):
        return True
    return _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)


def _ambiguous_school_tier1_candidate_allowed(job: FieldJob, url: str, document: SearchDocument) -> bool:
    if not _school_name_is_ambiguous(job):
        return True
    if _website_trust_tier(job, url) != "tier1":
        return True
    if _url_has_job_identity(job, url):
        return True
    if _url_has_job_identity(job, document.url or ""):
        return True
    if not _looks_like_directory_listing_url(url):
        return True
    return False


def _document_is_relevant(job: FieldJob, document: SearchDocument) -> bool:
    if document.provider in {"provenance", "chapter_website", "nationals_directory"}:
        return True
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or ""] if part))
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))


def _website_document_passes_relaxed_gate(job: FieldJob, document: SearchDocument) -> bool:
    if document.provider not in {"search_result", "search_page"}:
        return False
    if _website_trust_tier(job, document.url or "") != "tier1":
        return False
    if _website_document_looks_low_signal(document):
        return False
    combined = _document_match_text(document, limit=1200)
    if _school_has_conflicting_signal(job, combined):
        return False
    if _website_document_has_conflicting_org_signal(job, document):
        return False
    if _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined)):
        return True
    return _school_matches(job, combined) and any(marker in combined for marker in ("ifc", "greek", "fraternity", "student organization", "chapter"))


def _search_result_is_relevant(job: FieldJob, result: SearchResult) -> bool:
    combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))


def _search_result_is_useful(job: FieldJob, result: SearchResult, target: str) -> bool:
    combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")

    if target == "website":
        if _is_disallowed_website_candidate(result.url):
            return False
        if _website_candidate_looks_low_signal(result.url):
            return False
        if (
            _website_trust_tier(job, result.url) == "tier1"
            and not _ambiguous_school_tier1_candidate_allowed(
                job,
                result.url,
                SearchDocument(
                    text=result.snippet,
                    links=[result.url],
                    url=result.url,
                    title=result.title,
                    provider="search_result",
                ),
            )
        ):
            return False
        if _school_has_conflicting_signal(job, combined):
            return False
        if _text_has_conflicting_org_phrase(job, _normalized_match_text(result.title)):
            return False
        if _search_result_is_relevant(job, result):
            return True
        if _website_trust_tier(job, result.url) == "tier1":
            if any(marker in combined for marker in _LOW_SIGNAL_AFFILIATION_MARKERS):
                return False
            if not _school_matches(job, combined):
                return False
            if not any(marker in combined for marker in _OFFICIAL_AFFILIATION_MARKERS):
                return False
            return _fraternity_matches(job, combined) or _chapter_matches(job, combined)
        return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))

    if target == "email":
        hostname = (urlparse(result.url).netloc or "").lower()
        if hostname in _LOW_SIGNAL_EMAIL_RESULT_HOSTS or any(hostname.endswith(f".{blocked}") for blocked in _LOW_SIGNAL_EMAIL_RESULT_HOSTS):
            return False
        if _website_trust_tier(job, result.url) == "tier1" and any(marker in combined for marker in _LOW_SIGNAL_AFFILIATION_MARKERS):
            return False
        if _search_result_is_relevant(job, result):
            return True
        if _website_trust_tier(job, result.url) == "tier1" and (_school_matches(job, combined) or _fraternity_matches(job, combined)):
            return True
        return any(marker in combined for marker in ("contact", "email", "officer", "leadership")) and (_school_matches(job, combined) or _fraternity_matches(job, combined))

    if target != "instagram":
        return True
    hostname = (urlparse(result.url).netloc or "").lower()
    if hostname in _LOW_SIGNAL_INSTAGRAM_RESULT_HOSTS or any(hostname.endswith(f".{blocked}") for blocked in _LOW_SIGNAL_INSTAGRAM_RESULT_HOSTS):
        return False

    instagram_url = _normalize_instagram_candidate(result.url)
    if instagram_url:
        handle_score = _instagram_handle_match_score(instagram_url, job)
        if handle_score >= 2:
            return True
        if _search_result_is_relevant(job, result):
            return True
        combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
        if handle_score >= 1 and (_fraternity_matches(job, combined) or _school_matches(job, combined)):
            return True
        return False

    combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
    mentions_instagram = any(marker in combined for marker in ("instagram", "insta", "ig ", " ig"))
    if not mentions_instagram:
        return False
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))

def _should_fetch_search_result_page(job: FieldJob, result: SearchResult, target: str) -> bool:
    if target == "instagram":
        return False
    if target == "email":
        if _website_trust_tier(job, result.url) == "tier1":
            return True
        lowered = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
        if any(marker in lowered for marker in ("contact", "email", "officer", "leadership", "board", "about", "ifc", "greek life")):
            return True
        website_host = (urlparse(_current_website_url(job) or "").netloc or "").lower()
        result_host = (urlparse(result.url).netloc or "").lower()
        if website_host and (result_host == website_host or result_host.endswith(f".{website_host}")):
            return True
        return False
    return True


def _email_domain(email: str) -> str:
    if "@" not in email:
        return ""
    return email.rsplit("@", 1)[-1].strip().lower()


def _email_domain_matches_known_school_or_website(job: FieldJob, domain: str) -> bool:
    if not domain:
        return False
    if domain.endswith(".edu"):
        return True

    website_host = (urlparse(_current_website_url(job) or "").netloc or "").lower()
    if website_host and (domain == website_host or domain.endswith(f".{website_host}")):
        return True

    source_host = (urlparse(job.source_base_url or "").netloc or "").lower()
    if source_host and (domain == source_host or domain.endswith(f".{source_host}")):
        return True

    for campus_domain in _campus_domains(job):
        if domain == campus_domain or domain.endswith(f".{campus_domain}"):
            return True

    return False


def _email_context_overlap_score(job: FieldJob, email: str, document: SearchDocument) -> int:
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or "", email] if part))
    score = 0
    if _school_matches(job, combined):
        score += 2
    if _fraternity_matches(job, combined):
        score += 2
    if _chapter_matches(job, combined):
        score += 1
    if _email_domain_matches_known_school_or_website(job, _email_domain(email)):
        score += 1
    return score


def _email_looks_relevant_to_job(email: str, job: FieldJob, *, document: SearchDocument | None = None) -> bool:
    lowered = email.lower()
    normalized_email = _normalized_match_text(lowered)
    if _fraternity_matches(job, normalized_email):
        return True

    domain = _email_domain(email)
    if _email_domain_matches_known_school_or_website(job, domain):
        return True

    school_tokens = _significant_tokens(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    if any(token in lowered for token in school_tokens):
        return True

    if document is None:
        return False

    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or "", email] if part))
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))


def _document_match_text(document: SearchDocument, *, limit: int = 1600) -> str:
    return _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:limit], document.url or ""] if part))


def _website_document_looks_low_signal(document: SearchDocument) -> bool:
    combined = _document_match_text(document, limit=900)
    return any(marker in combined for marker in _LOW_SIGNAL_AFFILIATION_MARKERS)


def _website_candidate_looks_low_signal(url: str, document: SearchDocument | None = None) -> bool:
    parsed = urlparse(url)
    path_text = _normalized_match_text(f"{parsed.netloc} {parsed.path} {parsed.query}")
    if any(marker in path_text for marker in _LOW_SIGNAL_WEBSITE_PATH_MARKERS):
        return True
    if document is None:
        return False
    title_text = _normalized_match_text(" ".join(part for part in [document.title or "", document.url or ""] if part))
    return any(marker in title_text for marker in ("one book", "grade report", "architectural journalism", "terminology"))


def _website_document_has_official_chapter_context(job: FieldJob, document: SearchDocument) -> bool:
    combined = _document_match_text(document, limit=1600)
    if _school_has_conflicting_signal(job, combined):
        return False
    if _website_document_has_conflicting_org_signal(job, document):
        return False
    if not _school_matches(job, combined):
        return False
    if not (_fraternity_matches(job, combined) or _chapter_matches(job, combined)):
        return False
    return any(marker in combined for marker in _OFFICIAL_AFFILIATION_MARKERS)


def _search_page_link_has_website_context(job: FieldJob, document: SearchDocument, candidate_url: str) -> bool:
    if document.provider != "search_page" or not document.html:
        return False
    document_url = document.url or ""
    if not document_url:
        return False
    candidate_normalized = _normalize_url(urljoin(document_url, candidate_url))
    soup = BeautifulSoup(document.html, "html.parser")
    for node in soup.select("a[href]"):
        href = (node.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(document_url, href)
        if _normalize_url(absolute) != candidate_normalized:
            continue
        anchor_text = _normalized_match_text(node.get_text(" ", strip=True))
        parent_text = ""
        parent = getattr(node, "parent", None)
        if parent is not None and getattr(parent, "name", None) not in {"body", "html"}:
            parent_text = _normalized_match_text(parent.get_text(" ", strip=True)[:400])
        context = " ".join(part for part in [anchor_text, parent_text] if part)
        if any(marker in context for marker in _WEBSITE_LINK_CUE_MARKERS):
            return True
        if _fraternity_matches(job, context) and (_school_matches(job, context) or _chapter_matches(job, context)):
            return True
    return False


def _email_local_part_has_identity(email: str, job: FieldJob) -> bool:
    local_part = _compact_text(email.split("@", 1)[0])
    if not local_part:
        return False

    fraternity_display = _display_name(job.fraternity_slug)
    fraternity_compact = _compact_text(fraternity_display)
    fraternity_initials = _initialism(fraternity_display)
    if fraternity_compact and fraternity_compact in local_part:
        return True
    if fraternity_initials and len(fraternity_initials) >= 2 and fraternity_initials in local_part:
        return True

    school_initials = _school_initials(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    if school_initials and len(school_initials) >= 3 and school_initials in local_part:
        return True

    chapter_tokens = _significant_tokens(job.chapter_name)
    return any(token in local_part for token in chapter_tokens if len(token) >= 4)


def _email_local_part_looks_personal(email: str) -> bool:
    local_part = email.split("@", 1)[0].lower()
    return bool(re.fullmatch(r"[a-z]{2,}[._-][a-z]{2,}[0-9]{0,4}", local_part))


def _email_local_part_looks_generic_office(email: str) -> bool:
    local_part = email.split("@", 1)[0].lower()
    if local_part in _GENERIC_EMAIL_PREFIXES:
        return True
    return any(marker in local_part for marker in _GENERIC_OFFICE_EMAIL_MARKERS)


def _email_document_has_contact_context(document: SearchDocument) -> bool:
    combined = _document_match_text(document, limit=1200)
    return any(marker in combined for marker in _EMAIL_ROLE_MARKERS)

def _instagram_handle_match_score(instagram_url: str, job: FieldJob) -> int:
    handle = _compact_text(instagram_url.rsplit("/", 1)[-1])
    score = 0
    fraternity_compact = _compact_text(_display_name(job.fraternity_slug))
    fraternity_initials = _initialism(_display_name(job.fraternity_slug))
    school_tokens = _significant_tokens(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    school_initials = _school_initials(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    chapter_compact = _compact_text(job.chapter_name)
    chapter_initials = _initialism(job.chapter_name)

    if fraternity_compact and fraternity_compact in handle:
        score += 2
    elif fraternity_initials and len(fraternity_initials) >= 2 and fraternity_initials in handle:
        score += 1

    if school_initials and len(school_initials) >= 3 and school_initials in handle:
        score += 2
    elif any(token in handle for token in school_tokens if len(token) >= 5):
        score += 1

    if _has_nongeneric_chapter_signal(job):
        if chapter_compact and len(chapter_compact) >= 4 and chapter_compact in handle:
            score += 1
        elif chapter_initials and len(chapter_initials) >= 2 and chapter_initials in handle:
            score += 1

    return score


def _instagram_handle_has_fraternity_token(instagram_url: str, job: FieldJob) -> bool:
    handle = _compact_text(instagram_url.rsplit("/", 1)[-1])
    fraternity_compact = _compact_text(_display_name(job.fraternity_slug))
    fraternity_initials = _initialism(_display_name(job.fraternity_slug))
    if fraternity_compact and fraternity_compact in handle:
        return True
    return bool(fraternity_initials and len(fraternity_initials) >= 2 and fraternity_initials in handle)


def _instagram_candidate_text(document: SearchDocument, instagram_url: str) -> str:
    return _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or "", instagram_url] if part))


def _has_nongeneric_chapter_signal(job: FieldJob) -> bool:
    if _is_generic_greek_letter_chapter_name(job.chapter_name):
        return False
    return bool(_chapter_signal_tokens(job))


def _normalized_greek_chapter_designation(value: str | None) -> str:
    if not _is_generic_greek_letter_chapter_name(value):
        return ""
    return _normalized_match_text(value)


def _extract_greek_chapter_designations(text: str) -> set[str]:
    greek = "|".join(sorted(_GREEK_LETTER_TOKENS, key=len, reverse=True))
    pattern = re.compile(rf"\b(?:{greek})(?:\s+(?:{greek})){{0,2}}(?=\s+chapter\b)")
    return {match.group(0).strip() for match in pattern.finditer(text)}


def _extract_greek_org_phrases(text: str) -> set[str]:
    greek = "|".join(sorted(_GREEK_LETTER_TOKENS, key=len, reverse=True))
    pattern = re.compile(rf"\b(?:{greek})(?:\s+(?:{greek})){{1,3}}\b")
    return {match.group(0).strip() for match in pattern.finditer(text)}


def _text_has_conflicting_org_phrase(job: FieldJob, text: str) -> bool:
    canonical_target = _normalized_match_text(_canonical_fraternity_display(job.fraternity_slug))
    if not text or not canonical_target:
        return False
    if canonical_target in text:
        return False
    for phrase in _extract_greek_org_phrases(text):
        if phrase == canonical_target:
            continue
        return True
    return False


def _chapter_designation_signal(job: FieldJob, text: str) -> int:
    expected = _normalized_greek_chapter_designation(job.chapter_name)
    if not expected:
        return 0
    found = _extract_greek_chapter_designations(text)
    if not found:
        return 0
    if expected in found:
        return 2
    return -2


def _instagram_context_overlap_score(job: FieldJob, instagram_url: str, document: SearchDocument) -> int:
    combined = _instagram_candidate_text(document, instagram_url)
    score = 0
    if _school_matches(job, combined):
        score += 2
    if _fraternity_matches(job, combined):
        score += 2
    if _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined):
        score += 1
    designation_signal = _chapter_designation_signal(job, combined)
    if designation_signal > 0:
        score += designation_signal
    score += min(2, max(0, _instagram_handle_match_score(instagram_url, job) - 2))
    return score


def _instagram_has_generic_handle(instagram_url: str, job: FieldJob) -> bool:
    handle = _compact_text(instagram_url.rsplit("/", 1)[-1])
    fraternity_compact = _compact_text(_display_name(job.fraternity_slug))
    if not fraternity_compact:
        return False
    remainder = handle.removeprefix(fraternity_compact) if handle.startswith(fraternity_compact) else handle
    return handle == fraternity_compact or len(remainder) < 3


def _instagram_looks_institutional_or_directory_account(instagram_url: str, document: SearchDocument) -> bool:
    handle = _compact_text(instagram_url.rsplit("/", 1)[-1])
    if not handle:
        return False
    handle_markers = (
        "greeklife",
        "greeks",
        "ifc",
        "panhellenic",
        "studentlife",
        "studentaffairs",
        "campuslife",
        "reslife",
        "fraternitysorority",
        "fsa",
        "sfl",
    )
    if any(marker in handle for marker in handle_markers):
        return True
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or ""] if part))
    return any(
        marker in combined
        for marker in (
            "fraternity sorority affairs",
            "student affairs",
            "office of fraternity",
            "office of student life",
            "greek life office",
        )
    )


def _school_affiliation_document_is_trusted(job: FieldJob, document: SearchDocument) -> bool:
    host = (urlparse(document.url or "").netloc or "").lower()
    if not host:
        return False
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1600], document.url or ""] if part))
    if not _school_matches(job, combined):
        return False
    campus_domains = _campus_domains(job)
    if host.endswith(".edu"):
        return True
    if host in campus_domains or any(host.endswith(f".{domain}") for domain in campus_domains):
        return True
    return "ifc" in host


def _looks_like_official_school_affiliation_page(document: SearchDocument) -> bool:
    lowered = (document.text or "").lower()
    markers = (
        "ifc fraternities",
        "recognized chapters",
        "fraternity chapters",
        "fraternities",
        "greek life",
        "greek organizations",
        "chapters",
    )
    return any(marker in lowered for marker in markers)


def _instagram_document_is_relevant(job: FieldJob, document: SearchDocument) -> bool:
    if document.provider in {"provenance", "chapter_website", "nationals_directory"}:
        return True
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or ""] if part))
    chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
    if _fraternity_matches(job, combined) and (_school_matches(job, combined) or chapter_match):
        return True
    for link in document.links or [document.url or ""]:
        normalized = _normalize_instagram_candidate(link)
        if not normalized:
            continue
        handle_score = _instagram_handle_match_score(normalized, job)
        if handle_score >= 4:
            return True
        if handle_score >= 3 and _school_matches(job, combined):
            return True
    return False


def _instagram_looks_relevant_to_job(instagram_url: str, job: FieldJob, *, document: SearchDocument | None = None) -> bool:
    handle_score = _instagram_handle_match_score(instagram_url, job)
    if handle_score >= 4:
        return True
    if handle_score >= 3 and _has_nongeneric_chapter_signal(job) and _instagram_handle_has_fraternity_token(instagram_url, job):
        return True
    if document is None:
        return False
    combined = _instagram_candidate_text(document, instagram_url)
    if _instagram_has_conflicting_org_signal(job, combined) and handle_score < 5:
        return False
    chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
    if document.provider in {"provenance", "chapter_website", "nationals_directory"} and (chapter_match or handle_score >= 1):
        return True
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or chapter_match)


def _instagram_has_conflicting_org_signal(job: FieldJob, text: str) -> bool:
    canonical_target = _normalized_match_text(_display_name(job.fraternity_slug))
    if not text:
        return False
    for marker, canonical_other in _INSTAGRAM_CONFLICT_MARKERS.items():
        if marker not in text:
            continue
        if canonical_other in canonical_target:
            continue
        if canonical_target and canonical_target in text:
            continue
        return True
    return False


def _website_document_has_conflicting_org_signal(job: FieldJob, document: SearchDocument) -> bool:
    title_text = _normalized_match_text(document.title or "")
    if _text_has_conflicting_org_phrase(job, title_text):
        return True
    document_text = _document_match_text(document, limit=300)
    if _text_has_conflicting_org_phrase(job, document_text) and not _fraternity_matches(job, document_text):
        return True
    return False


def _should_skip_search_page_fetch(url: str) -> bool:
    parsed = urlparse(url)
    hostname = (parsed.netloc or "").lower()
    return hostname in _BLOCKED_WEBSITE_HOSTS or any(hostname.endswith(f".{blocked}") for blocked in _BLOCKED_WEBSITE_HOSTS)


def _looks_like_directory_listing_url(url: str) -> bool:
    lowered = url.lower()
    directory_markers = (
        "studentorg",
        "student-org",
        "student-orgs",
        "organization",
        "organizations",
        "greek-life",
        "greeklife",
        "/greek/",
        "/orgs/",
        "/clubs/",
        "/chapter/",
        "/chapters/",
        "/fsl/",
    )
    return any(marker in lowered for marker in directory_markers)


def _looks_like_document_asset_url(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    return any(path.endswith(extension) for extension in _DOCUMENT_URL_EXTENSIONS)


def _looks_like_generic_site_root(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").strip("/")
    return not path and not (parsed.query or "").strip()


def _school_exact_phrase_present(job: FieldJob, text: str) -> bool:
    university = job.university_name or str(job.payload.get("candidateSchoolName") or "")
    phrase = _normalized_match_text(university)
    return bool(phrase and phrase in text)


def _tier1_website_candidate_has_specificity(job: FieldJob, url: str, document: SearchDocument) -> bool:
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1600], document.url or "", url] if part))
    url_text = _normalized_match_text(f"{urlparse(url).netloc} {urlparse(url).path} {urlparse(url).query}")
    path_has_org_marker = any(
        marker in url_text
        for marker in (
            "fraternity",
            "sorority",
            "greek",
            "ifc",
            "chapter",
            "chapters",
            "student organization",
            "student organizations",
            "organization",
            "organizations",
            "club",
            "clubs",
            "fsl",
            "council",
        )
    )
    path_has_identity = _fraternity_matches(job, url_text) or _chapter_matches(job, url_text)
    if _website_document_looks_low_signal(document):
        return False
    if _website_candidate_looks_low_signal(url, document):
        return False
    if _school_has_conflicting_signal(job, combined):
        return False
    if _website_document_has_conflicting_org_signal(job, document):
        return False
    if not _ambiguous_school_tier1_candidate_allowed(job, url, document):
        return False
    if _normalize_url(url) == _normalize_url(document.url or "") and not _website_document_has_official_chapter_context(job, document):
        return False
    if path_has_identity and (_school_exact_phrase_present(job, combined) or _school_matches(job, combined)):
        return True
    if path_has_org_marker and _fraternity_matches(job, combined) and (_school_exact_phrase_present(job, combined) or _chapter_matches(job, combined)):
        return True
    if _looks_like_directory_listing_url(url) and _fraternity_matches(job, combined) and (_school_exact_phrase_present(job, combined) or _chapter_matches(job, combined)):
        return True
    return False


def _candidate_is_source_domain(url: str, job: FieldJob) -> bool:
    candidate_host = (urlparse(url).netloc or "").lower()
    source_host = (urlparse(job.source_base_url or "").netloc or "").lower()
    return bool(candidate_host and source_host and (candidate_host == source_host or candidate_host.endswith(f".{source_host}")))


def _trusted_directory_external_candidate(job: FieldJob, candidate_url: str, document: SearchDocument) -> bool:
    if document.provider != "search_page":
        return False
    if _website_trust_tier(job, document.url or "") != "tier1":
        return False
    if _website_candidate_looks_low_signal(candidate_url, document):
        return False
    if not _looks_like_directory_listing_url(document.url or "") and not _looks_like_official_school_affiliation_page(document):
        return False
    candidate_host = (urlparse(candidate_url).netloc or "").lower()
    document_host = (urlparse(document.url or "").netloc or "").lower()
    if not candidate_host or not document_host or candidate_host == document_host:
        return False
    if candidate_host.endswith(".edu"):
        return False
    compact_target = _compact_text(_display_name(job.fraternity_slug))
    compact_candidate = _compact_text(candidate_url)
    if compact_target and compact_target in compact_candidate:
        return True
    return False


def _is_safe_related_website_url(job: FieldJob, url: str) -> bool:
    current_website = _current_website_url(job)
    if current_website and _normalize_url(current_website) == _normalize_url(url):
        return True
    if _is_disallowed_website_candidate(url):
        return False

    normalized_candidate = _normalize_url(url)
    source_base_url = (job.source_base_url or "").strip()
    source_list_url = str(job.payload.get("sourceListUrl") or "").strip()
    if source_base_url and _normalize_url(source_base_url) == normalized_candidate:
        return False
    if source_list_url and _normalize_url(source_list_url) == normalized_candidate:
        return False

    parsed_candidate = urlparse(url)
    candidate_path = (parsed_candidate.path or "").lower().strip("/")
    if any(marker in candidate_path for marker in _GENERIC_DIRECTORY_PATH_MARKERS):
        return False

    source_host = (urlparse(source_base_url).netloc or "").lower()
    candidate_host = (parsed_candidate.netloc or "").lower()
    if source_host and candidate_host and (candidate_host == source_host or candidate_host.endswith(f".{source_host}")):
        path_parts = [part for part in candidate_path.split("/") if part]
        if len(path_parts) <= 1:
            return False

    return _search_result_is_relevant(job, SearchResult(title="", url=url, snippet=url, provider="derived", rank=0))

def _is_disallowed_website_candidate(url: str) -> bool:
    parsed = urlparse(url)
    hostname = (parsed.netloc or "").lower()
    if hostname in _BLOCKED_WEBSITE_HOSTS:
        return True
    return any(hostname.endswith(f".{blocked}") for blocked in _BLOCKED_WEBSITE_HOSTS)

def _best_match(matches: list[CandidateMatch]) -> CandidateMatch | None:
    if not matches:
        return None
    deduped: dict[str, CandidateMatch] = {}
    for match in matches:
        key = match.value.lower()
        existing = deduped.get(key)
        if existing is None or match.confidence > existing.confidence:
            deduped[key] = match
    return max(deduped.values(), key=lambda item: item.confidence)



def _score_result_context(job: FieldJob, text: str) -> int:
    lowered = text.lower()
    keywords = _job_keywords(job)
    return sum(1 for keyword in keywords if keyword and keyword in lowered)



def _job_keywords(job: FieldJob) -> list[str]:
    keywords: list[str] = []
    for value in (job.fraternity_slug, job.chapter_name, job.chapter_slug, job.university_name):
        slug = _slugify(value)
        if not slug:
            continue
        keywords.extend(part for part in slug.split("-") if len(part) >= 3)
    return list(dict.fromkeys(keywords))



def _display_name(value: str | None) -> str:
    if value is None:
        return ""
    return value.replace("-", " ").strip()



def _slugify(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"^-+|-+$", "", text)



def _normalize_url(url: str) -> str:
    return url.strip().rstrip("/").lower()

def _campus_domains(job: FieldJob) -> list[str]:
    values: list[str] = []
    for key in ("campusDomains", "campusDomain", "schoolDomains"):
        raw = job.payload.get(key)
        if isinstance(raw, str):
            values.append(raw)
        elif isinstance(raw, list):
            values.extend(str(item) for item in raw if item)
    normalized: list[str] = []
    for value in values:
        host = (urlparse(value if "://" in value else f"https://{value}").netloc or value).lower().strip()
        if host:
            normalized.append(host)
    return list(dict.fromkeys(normalized))


def _website_trust_tier(job: FieldJob, url: str | None) -> str:
    if not url:
        return "unknown"
    parsed = urlparse(url if "://" in url else f"https://{url}")
    hostname = (parsed.netloc or "").lower()
    if not hostname:
        return "unknown"
    if hostname in _BLOCKED_WEBSITE_HOSTS or any(hostname.endswith(f".{blocked}") for blocked in _BLOCKED_WEBSITE_HOSTS):
        return "blocked"
    if hostname in _TIER2_WEBSITE_HOSTS or any(hostname.endswith(f".{domain}") for domain in _TIER2_WEBSITE_HOSTS):
        return "tier2"
    if hostname.endswith(".edu"):
        return "tier1"
    campus_domains = _campus_domains(job)
    if hostname in campus_domains or any(hostname.endswith(f".{domain}") for domain in campus_domains):
        return "tier1"
    source_host = (urlparse(job.source_base_url or "").netloc or "").lower()
    if source_host and (hostname == source_host or hostname.endswith(f".{source_host}")):
        return "tier1"
    return "unknown"


def _normalize_greedy_collect_mode(value: str | None) -> str:
    normalized = (value or _GREEDY_COLLECT_NONE).strip().lower()
    if normalized in {_GREEDY_COLLECT_NONE, _GREEDY_COLLECT_PASSIVE, _GREEDY_COLLECT_BFS}:
        return normalized
    return _GREEDY_COLLECT_NONE


def _should_follow_nationals_link(url: str, source_host: str, mode: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if not host:
        return False
    if not (host == source_host or host.endswith(f".{source_host}")):
        return False
    path = (parsed.path or "").lower().strip("/")
    if not path:
        return True
    if any(marker in path for marker in _NATIONALS_LINK_MARKERS):
        return True
    parts = [part for part in path.split("/") if part]
    if len(parts) == 1 and parts[0] in _STATE_ABBREVIATIONS:
        return True
    if mode == _GREEDY_COLLECT_BFS and len(parts) <= 2:
        compact = "".join(parts)
        if compact in _STATE_ABBREVIATIONS:
            return True
        if parts and any(marker in parts[0] for marker in _NATIONALS_LINK_MARKERS):
            return True
    return False


def _extract_nationals_chapter_entries(document: SearchDocument) -> list[NationalsChapterEntry]:
    if not document.html:
        return []
    soup = BeautifulSoup(document.html, "html.parser")
    entries: list[NationalsChapterEntry] = []
    seen_signatures: set[str] = set()

    for heading in soup.select("h1, h2, h3, h4, h5, strong"):
        heading_text = heading.get_text(" ", strip=True)
        if not _looks_like_chapter_heading(heading_text):
            continue
        block_text, links = _chapter_heading_block(heading)
        if not _block_has_contact_signal(block_text):
            continue
        entry = _parse_nationals_entry_block(heading_text, block_text, links, document.url or "")
        if entry is None:
            continue
        signature = f"{_compact_text(entry.chapter_name)}|{_compact_text(entry.university_name or '')}|{_compact_text(entry.source_snippet)}"
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        entries.append(entry)
    return entries


def _looks_like_chapter_heading(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered or "chapter" not in lowered:
        return False
    if len(lowered) > 120:
        return False
    if not lowered.endswith("chapter"):
        return False
    if any(marker in lowered for marker in _NATIONALS_HEADING_BLOCKLIST_MARKERS):
        return False
    if re.search(r"https?://|www\.|@|mailto:", lowered):
        return False
    tokens = [token for token in re.split(r"[^a-z0-9]+", lowered) if token]
    if len(tokens) < 2 or len(tokens) > 10:
        return False
    return True


def _chapter_heading_block(heading) -> tuple[str, list[str]]:
    blocks: list[str] = [heading.get_text(" ", strip=True)]
    links: list[str] = []
    sibling_anchor = heading

    parent = heading.parent
    if parent is not None and parent.name in {"article", "section", "div", "li"}:
        parent_text = parent.get_text(" ", strip=True)
        if parent.name != "body" and len(parent_text) <= 500:
            blocks.append(parent_text)
            links.extend(node.get("href", "") for node in parent.select("a[href]"))
            joined = " ".join(blocks)
            if _block_has_contact_signal(joined):
                return joined, [link for link in links if link]
            blocks = [blocks[0]]
            links = []
            sibling_anchor = parent
            ancestor = parent
            for _ in range(2):
                if _anchor_has_following_content(ancestor):
                    sibling_anchor = ancestor
                    break
                parent_candidate = getattr(ancestor, "parent", None)
                if parent_candidate is None or getattr(parent_candidate, "name", None) not in {"article", "section", "div", "li"}:
                    break
                ancestor = parent_candidate

    running_chars = len(blocks[0])
    for sibling in sibling_anchor.next_siblings:
        sibling_name = getattr(sibling, "name", None)
        if sibling_name and str(sibling_name).lower() in {"h1", "h2", "h3", "h4", "h5", "strong"}:
            break
        if hasattr(sibling, "get_text"):
            sibling_text = sibling.get_text(" ", strip=True)
            if sibling_text:
                blocks.append(sibling_text)
                running_chars += len(sibling_text)
            if hasattr(sibling, "select"):
                links.extend(node.get("href", "") for node in sibling.select("a[href]"))
        else:
            text = str(sibling).strip()
            if text:
                blocks.append(text)
                running_chars += len(text)
        if running_chars >= 700:
            break
    return " ".join(blocks), [link for link in links if link]


def _anchor_has_following_content(anchor) -> bool:
    if anchor is None:
        return False
    checked = 0
    for sibling in anchor.next_siblings:
        checked += 1
        if checked > 6:
            break
        if hasattr(sibling, "select") and sibling.select("a[href]"):
            return True
        text = sibling.get_text(" ", strip=True) if hasattr(sibling, "get_text") else str(sibling).strip()
        if len(text) >= 24:
            return True
    return False


def _extract_nationals_script_seed_urls(document: SearchDocument, source_host: str) -> list[str]:
    html = document.html or ""
    if not html:
        return []
    if "chapter-directory" not in html.lower() and "uscanada_config" not in html.lower():
        return []

    links: list[str] = []
    seen: set[str] = set()
    for raw in _NATIONALS_SCRIPT_URL_RE.findall(html):
        candidate = raw.replace("\\/", "/").strip()
        if not candidate:
            continue
        absolute = urljoin(document.url or "", candidate)
        normalized = _normalize_url(absolute)
        if normalized in seen:
            continue
        parsed = urlparse(absolute)
        host = (parsed.netloc or "").lower()
        if not host:
            continue
        if not (host == source_host or host.endswith(f".{source_host}")):
            continue
        path = (parsed.path or "").lower()
        if "chapter-directory" not in path and "chapters" not in path:
            continue
        seen.add(normalized)
        links.append(absolute)
    return links


def _block_has_contact_signal(value: str) -> bool:
    lowered = value.lower()
    if any(marker in lowered for marker in _NATIONALS_CONTACT_CUE_MARKERS):
        return True
    if "mailto:" in lowered:
        return True
    if _EMAIL_RE.search(value):
        return True
    return bool(re.search(r"\b[a-z][a-z0-9.-]+\.(?:org|com|edu|ca|net)\b", lowered))


def _parse_nationals_entry_block(heading_text: str, block_text: str, links: list[str], source_url: str) -> NationalsChapterEntry | None:
    heading_clean = heading_text.strip()
    if not heading_clean:
        return None
    chapter_name = re.sub(r"\s*chapter\s*$", "", heading_clean, flags=re.IGNORECASE).strip()
    chapter_name = " ".join(chapter_name.split())
    if not chapter_name:
        return None
    lowered_chapter_name = chapter_name.lower()
    if len(chapter_name) > 120:
        return None
    if any(marker in lowered_chapter_name for marker in ("http://", "https://", "website:", "instagram", "facebook", "twitter", "@")):
        return None

    university_name = _to_title_if_upper(chapter_name)
    if university_name.lower().startswith("the "):
        university_name = university_name[4:]
    if len(university_name) > 160:
        return None

    normalized_links = [urljoin(source_url, link.strip()) for link in links if link and link.strip()]
    source_host = (urlparse(source_url).netloc or "").lower()
    instagram_url: str | None = None
    website_url: str | None = None
    contact_email: str | None = None
    website_label_present = "website" in block_text.lower()

    for link in normalized_links:
        insta = _normalize_instagram_candidate(link)
        if insta and not instagram_url:
            instagram_url = insta
            continue
        if link.lower().startswith("mailto:") and not contact_email:
            mail_match = _MAILTO_RE.search(link)
            if mail_match:
                contact_email = unquote(mail_match.group(1))
            continue
        lowered_link = link.lower()
        if any(host in lowered_link for host in ("facebook.com", "twitter.com", "x.com", "linkedin.com")):
            continue
        host = (urlparse(link).netloc or "").lower()
        is_external = bool(host) and host != source_host and not host.endswith(f".{source_host}")
        if website_label_present and is_external and link.lower().startswith(("http://", "https://")) and not website_url:
            website_url = link

    if not instagram_url:
        insta_match = _INSTAGRAM_RE.search(block_text)
        if insta_match:
            instagram_url = _normalize_instagram_candidate(insta_match.group(0))
        else:
            handle_match = _INSTAGRAM_HANDLE_HINT_RE.search(block_text) or _INSTAGRAM_NEARBY_HANDLE_RE.search(block_text)
            if handle_match:
                instagram_url = _normalize_instagram_candidate(handle_match.group(1))

    if not contact_email:
        email_match = _EMAIL_RE.search(block_text)
        if email_match:
            contact_email = email_match.group(0)

    if not website_url and website_label_present:
        website_pattern = re.search(r"website\s*:\s*(https?://[^\s]+)", block_text, flags=re.IGNORECASE)
        if website_pattern:
            website_url = website_pattern.group(1).rstrip(".,;)")

    if not website_url:
        for link in normalized_links:
            lowered_link = link.lower()
            if not lowered_link.startswith(("http://", "https://")):
                continue
            if any(host in lowered_link for host in ("instagram.com", "facebook.com", "twitter.com", "x.com", "linkedin.com")):
                continue
            host = (urlparse(link).netloc or "").lower()
            is_external = bool(host) and host != source_host and not host.endswith(f".{source_host}")
            if is_external:
                website_url = link
                break

    source_snippet = " ".join(block_text.split())[:400]
    if len(source_snippet) < 18:
        return None
    confidence = 0.72
    if university_name:
        confidence += 0.06
    if website_url:
        confidence += 0.08
    if instagram_url:
        confidence += 0.08
    if contact_email:
        confidence += 0.06

    return NationalsChapterEntry(
        chapter_name=_to_title_if_upper(chapter_name),
        university_name=university_name or None,
        website_url=website_url,
        instagram_url=instagram_url,
        contact_email=contact_email,
        source_url=source_url,
        source_snippet=source_snippet,
        confidence=max(0.0, min(0.95, confidence)),
    )


def _to_title_if_upper(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        return stripped
    if stripped.upper() == stripped:
        return stripped.title()
    return stripped


def _nationals_entry_to_document(entry: NationalsChapterEntry) -> SearchDocument:
    links = [link for link in [entry.website_url, entry.instagram_url] if link]
    parts = [entry.chapter_name, entry.university_name or "", entry.source_snippet]
    if entry.contact_email:
        parts.append(entry.contact_email)
    return SearchDocument(
        text=" ".join(part for part in parts if part),
        links=links,
        url=entry.source_url,
        title=entry.chapter_name,
        provider="nationals_directory",
        query="nationals_directory",
    )


def _nationals_entry_match_score(job: FieldJob, entry: NationalsChapterEntry) -> int:
    combined = _normalized_match_text(
        " ".join(
            part
            for part in [entry.chapter_name, entry.university_name or "", entry.source_snippet, entry.source_url]
            if part
        )
    )
    if not combined:
        return 0
    if _school_has_conflicting_signal(job, combined):
        return 0
    if _text_has_conflicting_org_phrase(job, combined):
        return 0
    school_exact = _school_exact_phrase_present(job, combined)
    school_match = _school_matches(job, combined)
    chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
    if not school_match and not chapter_match:
        return 0
    score = 0
    if school_exact:
        score += 4
    elif school_match:
        score += 3
    if chapter_match:
        score += 2
    if _fraternity_matches(job, combined):
        score += 1
    return score


def _nationals_entry_is_ingestible(entry: NationalsChapterEntry) -> bool:
    if not entry.chapter_name or not entry.university_name:
        return False
    if len(entry.chapter_name) > 120 or len(entry.university_name) > 160:
        return False
    if not any([entry.website_url, entry.instagram_url, entry.contact_email]):
        return False
    if entry.confidence < 0.8:
        return False
    return True


def _discovered_field_states(chapter) -> dict[str, str]:
    states: dict[str, str] = {}
    if chapter.name:
        states["name"] = "found"
    if chapter.university_name:
        states["university_name"] = "found"
    if chapter.city:
        states["city"] = "found"
    if chapter.state:
        states["state"] = "found"
    if chapter.website_url:
        states["website_url"] = "found"
    if chapter.instagram_url:
        states["instagram_url"] = "found"
    if chapter.contact_email:
        states["contact_email"] = "found"
    return states


def _is_low_signal_university_name(value: str | None) -> bool:
    tokens = _significant_tokens(value)
    if len(tokens) >= 2:
        return False
    lowered = _normalized_match_text(value)
    if any(marker in lowered for marker in ("university", "college", "institute", "school")):
        return False
    return True



class RetryableJobError(Exception):
    def __init__(
        self,
        message: str,
        *,
        backoff_seconds: int | None = None,
        low_signal: bool = False,
        preserve_attempt: bool = False,
        reason_code: str = "retryable",
    ):
        super().__init__(message)
        self.backoff_seconds = backoff_seconds
        self.low_signal = low_signal
        self.preserve_attempt = preserve_attempt
        self.reason_code = reason_code


def _website_is_confident(job: FieldJob) -> bool:
    if not _current_website_url(job):
        return False
    state = (job.field_states or {}).get("website_url")
    return state not in {"low_confidence", "missing"}



















