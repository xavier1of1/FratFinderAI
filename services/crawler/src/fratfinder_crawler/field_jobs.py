from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import unquote, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from fratfinder_crawler.adaptive.policy import AdaptivePolicy
from fratfinder_crawler.candidate_sanitizer import (
    CandidateKind,
    sanitize_as_email,
    sanitize_as_instagram,
    sanitize_as_website,
    sanitize_candidate,
)
from fratfinder_crawler.field_job_support import (
    job_has_canonical_active_status,
    job_has_existing_instagram_support,
    job_supporting_page_ready,
)
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import (
    CONTACT_SPECIFICITY_AMBIGUOUS,
    CONTACT_SPECIFICITY_CHAPTER,
    CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
    CONTACT_SPECIFICITY_NATIONAL_GENERIC,
    CONTACT_SPECIFICITY_SCHOOL,
    EnrichmentObservation,
    ExtractedChapter,
    FieldJob,
    FIELD_RESOLUTION_CONFIRMED_ABSENT,
    FIELD_RESOLUTION_INACTIVE,
    FIELD_RESOLUTION_RESOLVED,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_TO_STATE_KEY,
    FIELD_TO_CHAPTER_COLUMN,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    PAGE_SCOPE_CHAPTER_SITE,
    PAGE_SCOPE_DIRECTORY,
    PAGE_SCOPE_NATIONALS_CHAPTER,
    PAGE_SCOPE_NATIONALS_GENERIC,
    PAGE_SCOPE_SCHOOL_AFFILIATION,
    PAGE_SCOPE_UNRELATED,
    ProvenanceRecord,
    ReviewItemCandidate,
    SourceRecord,
)
from fratfinder_crawler.normalization import classify_chapter_validity, normalize_record
from fratfinder_crawler.precision_tools import (
    PrecisionDecision,
    tool_campus_greek_life_policy,
    tool_directory_block_matcher,
    tool_official_domain_verifier,
    tool_school_chapter_list_validator,
    tool_site_scope_classifier,
)
from fratfinder_crawler.search import SearchClient, SearchResult, SearchUnavailableError
from fratfinder_crawler.social import (
    InstagramCandidateBank,
    InstagramSourceType,
    audit_existing_instagram_candidate,
    build_chapter_instagram_identity,
    build_instagram_search_queries,
    candidate_from_chapter_evidence,
    score_instagram_candidate,
)
from fratfinder_crawler.social.instagram_resolver import instagram_write_threshold
from fratfinder_crawler.status import (
    CampusSourceDocument,
    ChapterStatusDecision,
    ChapterStatusEvidence,
    ChapterStatusFinal,
    SchoolRecognitionStatus,
    build_campus_status_index,
    chapter_activity_status_from_decision,
    decide_chapter_status,
    school_policy_status_from_decision,
)
from fratfinder_crawler.status.evidence_repository import status_decision_metadata
from fratfinder_crawler.status.national_capabilities import infer_national_status_from_page

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
_GENERIC_EMAIL_PREFIXES = {
    "info",
    "contact",
    "admin",
    "office",
    "hello",
    "membership",
    "national",
    "nationals",
    "headquarters",
    "hq",
    "ihq",
}
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
    "fsl",
    "chapter profile",
    "chapter profiles",
    "chapters",
    "clubs organizations",
    "council",
    "find a student org",
    "fraternities",
    "fraternity and sorority",
    "fraternity student life",
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
    "admission",
    "admissions",
    "advisor",
    "fsl",
    "greeklife",
    "greek.life",
    "graduateprogram",
    "graduateprograms",
    "ifc",
    "leadership",
    "ofsl",
    "osfl",
    "office",
    "operator",
    "reslife",
    "studentengagement",
    "studentaffairs",
    "studentinvolvement",
    "student.life",
    "studentlife",
    "studentorg",
    "studentorganization",
    "studentorganizations",
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
_INSTITUTION_NAME_MARKERS = (
    "university",
    "college",
    "institute",
    "academy",
    "school",
    "polytechnic",
    "state",
    "campus",
    "tech",
)
_INSTAGRAM_CONFLICT_MARKERS = {
    "tri sigma": "sigma sigma sigma",
    "sigma sigma sigma": "sigma sigma sigma",
    "trisigma": "sigma sigma sigma",
    "delta chi fraternity": "delta chi",
}
_NATIONAL_GENERIC_INSTAGRAM_MARKERS = {
    "hq",
    "ihq",
    "national",
    "nationals",
    "officialhq",
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
_STATE_KEY_TO_FIELD_JOB = {
    "website_url": FIELD_JOB_FIND_WEBSITE,
    "contact_email": FIELD_JOB_FIND_EMAIL,
    "instagram_url": FIELD_JOB_FIND_INSTAGRAM,
}


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
class ActivityValidationDecision:
    school_policy_status: str = "unknown"
    chapter_activity_status: str = "unknown"
    final_status: str = "unknown"
    school_recognition_status: str = "unknown"
    national_status: str = "unknown"
    evidence_url: str | None = None
    evidence_source_type: str | None = None
    reason_code: str | None = None
    source_snippet: str | None = None
    confidence: float = 0.0
    status_decision_id: str | None = None
    review_required: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


def _parse_document_markup(markup: str) -> BeautifulSoup:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        return BeautifulSoup(markup, "html.parser")


@dataclass(slots=True)
class AuthoritativeBundle:
    website_match: CandidateMatch | None = None
    email_match: CandidateMatch | None = None
    instagram_match: CandidateMatch | None = None
    website_confirmed_absent: bool = False
    authoritative_context_found: bool = False
    evidence_url: str | None = None
    evidence_source_type: str | None = None
    reason_code: str | None = None


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


def _map_scope_classifier_to_page_scope(decision: str, *, prefer_chapter_nationals: bool = False) -> str:
    normalized = str(decision or "").strip().lower()
    if normalized == "chapter_site":
        return PAGE_SCOPE_CHAPTER_SITE
    if normalized == "school_affiliation":
        return PAGE_SCOPE_SCHOOL_AFFILIATION
    if normalized == "nationals":
        return PAGE_SCOPE_NATIONALS_CHAPTER if prefer_chapter_nationals else PAGE_SCOPE_NATIONALS_GENERIC
    if normalized == "directory":
        return PAGE_SCOPE_DIRECTORY
    return PAGE_SCOPE_UNRELATED


def _contact_specificity_for_page_scope(page_scope: str) -> str:
    if page_scope == PAGE_SCOPE_CHAPTER_SITE:
        return CONTACT_SPECIFICITY_CHAPTER
    if page_scope == PAGE_SCOPE_SCHOOL_AFFILIATION:
        return CONTACT_SPECIFICITY_SCHOOL
    if page_scope == PAGE_SCOPE_NATIONALS_CHAPTER:
        return CONTACT_SPECIFICITY_NATIONAL_CHAPTER
    if page_scope == PAGE_SCOPE_NATIONALS_GENERIC:
        return CONTACT_SPECIFICITY_NATIONAL_GENERIC
    return CONTACT_SPECIFICITY_AMBIGUOUS


def _job_supporting_page_ready(job: FieldJob) -> bool:
    return job_supporting_page_ready(job)


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
        search_degraded_mode: bool = False,
        adaptive_policy: AdaptivePolicy | None = None,
        adaptive_runtime_mode: str | None = None,
        adaptive_policy_mode: str = "shadow",
        adaptive_policy_version: str | None = None,
        provider_window_state: dict[str, Any] | None = None,
        enrichment_observations_enabled: bool = True,
        validate_existing_instagram: bool = False,
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
        self._search_degraded_mode = bool(search_degraded_mode)
        self._adaptive_policy = adaptive_policy
        self._adaptive_runtime_mode = str(adaptive_runtime_mode or "shadow")
        self._adaptive_policy_mode = str(adaptive_policy_mode or "shadow")
        self._adaptive_policy_version = str(adaptive_policy_version or getattr(adaptive_policy, "policy_version", "") or "").strip() or None
        self._provider_window_state = dict(provider_window_state or {})
        self._enrichment_observations_enabled = bool(enrichment_observations_enabled)
        self._validate_existing_instagram = bool(validate_existing_instagram)
        self._search_errors_encountered = False
        self._search_queries_attempted = 0
        self._search_queries_failed = 0
        self._search_queries_succeeded = 0
        self._search_fanout_aborted = False
        self._search_skipped_due_to_degraded_mode = False
        self._last_provider_attempts: list[dict[str, object]] = []
        self._last_query_provider_attempts: list[dict[str, object]] = []
        self._last_search_failure_kind: str | None = None
        self._search_result_cache: dict[str, list[SearchResult]] = {}
        self._search_document_cache: dict[str, SearchDocument | None] = {}
        self._provenance_text_cache: dict[str, str] = {}
        self._candidate_rejection_counts: dict[str, int] = {}
        self._decision_trace: list[dict[str, str | int | float | bool | None]] = []
        self._chapter_search_queries: list[str] = []
        self._last_batch_metrics: dict[str, object] = {}
        self._enrichment_observations_logged = 0
        self._nationals_entries_cache: dict[str, list[NationalsChapterEntry]] = {}
        self._nationals_collect_attempted: set[str] = set()
        self._source_record_cache: dict[str, SourceRecord | None] = {}
        self._school_policy_cache: dict[str, ActivityValidationDecision] = {}
        self._chapter_activity_cache: dict[tuple[str, str], ActivityValidationDecision] = {}
        self._status_decision_cache: dict[str, ChapterStatusDecision] = {}
        self._authoritative_bundle_cache: dict[str, AuthoritativeBundle] = {}
        self._latest_provenance_context_cache: dict[str, dict[str, Any]] = {}
        self._verify_school_cache_hit_count = 0
        self._verify_school_official_url_reused_count = 0
        self._verify_school_provider_search_attempted_count = 0
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
        summary: dict[str, object] = {
            "processed": 0,
            "requeued": 0,
            "failed_terminal": 0,
            "provider_degraded_deferred": 0,
            "dependency_wait_deferred": 0,
            "supporting_page_resolved": 0,
            "supporting_page_contact_resolved": 0,
            "external_search_contact_resolved": 0,
            "chapter_search_queries": [],
            "enrichment_observations_logged": 0,
            "degraded_authoritative_claimed": 0,
            "verify_school_cache_hit": 0,
            "verify_school_official_url_reused": 0,
            "verify_school_provider_search_attempted": 0,
        }

        for _ in range(limit):
            job = self._repository.claim_next_field_job(
                self._worker_id,
                source_slug=self._source_slug,
                field_name=self._field_name,
                require_confident_website_for_email=self._require_confident_website_for_email,
                degraded_mode=self._search_degraded_mode,
            )
            if job is None:
                break
            job = replace(job, field_name=_STATE_KEY_TO_FIELD_JOB.get(job.field_name, job.field_name))
            if self._search_degraded_mode and job.field_name in {
                FIELD_JOB_VERIFY_SCHOOL,
                FIELD_JOB_VERIFY_WEBSITE,
                FIELD_JOB_FIND_INSTAGRAM,
                FIELD_JOB_FIND_EMAIL,
            }:
                summary["degraded_authoritative_claimed"] = int(summary["degraded_authoritative_claimed"]) + 1

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

            shadow_context = self._build_enrichment_policy_context(job)
            shadow_decisions = self._score_enrichment_shadow_decisions(shadow_context)

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
                summary["processed"] = int(summary["processed"]) + 1
                self._accumulate_process_metrics(summary, job, result)
                self._record_enrichment_observation(
                    job,
                    context=shadow_context,
                    decisions=shadow_decisions,
                    deterministic_action=self._deterministic_enrichment_action(job, context=shadow_context, result=result),
                    outcome=self._build_enrichment_outcome(job, result=result),
                )
                summary["enrichment_observations_logged"] = self._enrichment_observations_logged
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
                    summary["failed_terminal"] = int(summary["failed_terminal"]) + 1
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
                summary["requeued"] = int(summary["requeued"]) + 1
                if exc.reason_code == "provider_degraded":
                    summary["provider_degraded_deferred"] = int(summary["provider_degraded_deferred"]) + 1
                if exc.reason_code in {"dependency_wait", "website_required", "status_dependency_unmet"}:
                    summary["dependency_wait_deferred"] = int(summary["dependency_wait_deferred"]) + 1
                self._append_chapter_search_queries(summary, self._chapter_search_queries)
                self._record_enrichment_observation(
                    job,
                    context=shadow_context,
                    decisions=shadow_decisions,
                    deterministic_action=self._deterministic_enrichment_action(job, context=shadow_context, retry_error=exc),
                    outcome=self._build_enrichment_outcome(job, retry_error=exc, backoff_seconds=backoff_seconds),
                )
                summary["enrichment_observations_logged"] = self._enrichment_observations_logged
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
                summary["failed_terminal"] = int(summary["failed_terminal"]) + 1
                self._record_enrichment_observation(
                    job,
                    context=shadow_context,
                    decisions=shadow_decisions,
                    deterministic_action=self._deterministic_enrichment_action(job, context=shadow_context, unexpected_error=exc),
                    outcome=self._build_enrichment_outcome(job, unexpected_error=exc),
                )
                summary["enrichment_observations_logged"] = self._enrichment_observations_logged
                log_event(
                    self._logger,
                    "field_job_unexpected_failure",
                    field_job_id=job.id,
                    chapter_slug=job.chapter_slug,
                    field_name=job.field_name,
                    error=str(exc),
                )

        self._last_batch_metrics = dict(summary)
        self._last_batch_metrics["verify_school_cache_hit"] = self._verify_school_cache_hit_count
        self._last_batch_metrics["verify_school_official_url_reused"] = self._verify_school_official_url_reused_count
        self._last_batch_metrics["verify_school_provider_search_attempted"] = self._verify_school_provider_search_attempted_count
        return {
            "processed": int(summary["processed"]),
            "requeued": int(summary["requeued"]),
            "failed_terminal": int(summary["failed_terminal"]),
        }

    def consume_last_batch_metrics(self) -> dict[str, object]:
        metrics = dict(self._last_batch_metrics or {})
        self._last_batch_metrics = {}
        return metrics

    def _build_enrichment_policy_context(self, job: FieldJob) -> dict[str, object]:
        contact_resolution = job.payload.get("contactResolution") if isinstance(job.payload.get("contactResolution"), dict) else {}
        general_lane = (self._provider_window_state or {}).get("general_web_search")
        if not isinstance(general_lane, dict):
            general_lane = {}
        supporting_page_url = str(contact_resolution.get("supportingPageUrl") or "").strip()
        supporting_page_scope = str(contact_resolution.get("supportingPageScope") or contact_resolution.get("pageScope") or "").strip().lower()
        target_field_value = {
            FIELD_JOB_FIND_WEBSITE: job.website_url,
            FIELD_JOB_VERIFY_WEBSITE: job.website_url,
            FIELD_JOB_FIND_EMAIL: job.contact_email,
            FIELD_JOB_FIND_INSTAGRAM: job.instagram_url,
            FIELD_JOB_VERIFY_SCHOOL: job.university_name,
        }.get(job.field_name)
        prior_query_count = len(list(job.payload.get("provider_attempts") or [])) + int(job.payload.get("terminal_no_signal_count", 0) or 0)
        return {
            "field_type": job.field_name,
            "supporting_page_present": bool(_job_supporting_page_ready(job) or supporting_page_url),
            "supporting_page_scope": supporting_page_scope,
            "website_prerequisite_unmet": bool(job.field_name == FIELD_JOB_FIND_EMAIL and not _job_supporting_page_ready(job)),
            "school_validation_status": str(job.payload.get("schoolValidationStatus") or "").strip().lower(),
            "provider_window_healthy": bool(general_lane.get("healthy", False)),
            "provider_window_degraded": not bool(general_lane.get("healthy", False)),
            "prior_query_count": prior_query_count,
            "identity_complete": bool(job.chapter_name and job.university_name and job.fraternity_slug),
            "has_candidate_website": bool(job.website_url),
            "has_target_value": bool(target_field_value),
            "needs_authoritative_validation": bool(job.field_name in {FIELD_JOB_VERIFY_SCHOOL, FIELD_JOB_VERIFY_WEBSITE}),
            "timeout_risk": 1.0 if not bool(general_lane.get("healthy", False)) else 0.15,
            "requeue_risk": 0.8 if job.queue_state == "deferred" else 0.2,
        }

    def _score_enrichment_shadow_decisions(self, context: dict[str, object]) -> list[dict[str, object]]:
        if self._adaptive_policy is None:
            return []
        decisions = self._adaptive_policy.choose_action(
            [
                "parse_supporting_page",
                "verify_school",
                "verify_website",
                "search_web",
                "search_social",
                "defer",
                "stop_no_signal",
                "review_required",
            ],
            context=context,
            template_profile=None,
            mode="adaptive_shadow",
        )
        return [
            {
                "actionType": decision.action_type,
                "score": decision.score,
                "predictedReward": decision.predicted_reward,
                "scoreComponents": dict(decision.score_components or {}),
            }
            for decision in decisions[:4]
        ]

    def _deterministic_enrichment_action(
        self,
        job: FieldJob,
        *,
        context: dict[str, object],
        result: FieldJobResult | None = None,
        retry_error: "RetryableJobError" | None = None,
        unexpected_error: Exception | None = None,
    ) -> str:
        if retry_error is not None:
            if retry_error.reason_code in {"provider_degraded", "transient_network", "dependency_wait", "website_required", "provider_low_signal", "status_dependency_unmet"}:
                return "defer"
            if retry_error.low_signal:
                return "stop_no_signal"
            return "search_social" if job.field_name == FIELD_JOB_FIND_INSTAGRAM else "search_web"
        if unexpected_error is not None:
            return "review_required"
        if result is not None and result.review_item is not None:
            return "review_required"
        if bool(context.get("supporting_page_present")):
            return "parse_supporting_page"
        if job.field_name == FIELD_JOB_VERIFY_SCHOOL:
            return "verify_school"
        if job.field_name in {FIELD_JOB_FIND_WEBSITE, FIELD_JOB_VERIFY_WEBSITE}:
            return "verify_website" if bool(context.get("has_candidate_website")) or bool(context.get("needs_authoritative_validation")) else "search_web"
        if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
            return "search_social"
        if job.field_name == FIELD_JOB_FIND_EMAIL and bool(context.get("website_prerequisite_unmet")):
            return "defer"
        return "search_web"

    def _build_enrichment_outcome(
        self,
        job: FieldJob,
        *,
        result: FieldJobResult | None = None,
        retry_error: "RetryableJobError" | None = None,
        unexpected_error: Exception | None = None,
        backoff_seconds: int | None = None,
    ) -> dict[str, object]:
        if retry_error is not None:
            return {
                "finalState": "requeued",
                "reasonCode": retry_error.reason_code,
                "backoffSeconds": backoff_seconds,
                "businessSignals": {
                    "provider_waste": retry_error.reason_code in {"provider_degraded", "transient_network", "provider_low_signal"},
                    "repair_requeue_without_progress": retry_error.reason_code == "queued_for_entity_repair",
                },
            }
        if unexpected_error is not None:
            return {
                "finalState": "failed_terminal",
                "reasonCode": "unexpected_error",
                "error": str(unexpected_error),
                "businessSignals": {
                    "review_only_run": True,
                },
            }
        payload = dict((result.completed_payload if result is not None else {}) or {})
        resolution = payload.get("resolutionEvidence") if isinstance(payload.get("resolutionEvidence"), dict) else {}
        status = str(payload.get("status") or "").strip().lower()
        field_name = str(payload.get("field") or FIELD_TO_CHAPTER_COLUMN.get(job.field_name, job.field_name) or "")
        specificity = str(resolution.get("contactSpecificity") or "").strip()
        safe_specificity = {
            CONTACT_SPECIFICITY_CHAPTER,
            CONTACT_SPECIFICITY_SCHOOL,
            CONTACT_SPECIFICITY_NATIONAL_CHAPTER,
        }
        business_signals = {
            "validated_active": False,
            "validated_inactive": False,
            "chapter_safe_website": False,
            "chapter_safe_email": False,
            "chapter_safe_instagram": False,
            "complete_row": False,
            "review_only_run": bool(result.review_item is not None) if result is not None else False,
            "school_office_contact_attempt": specificity == CONTACT_SPECIFICITY_SCHOOL and field_name in {"contact_email", "instagram_url"},
            "national_generic_contact_attempt": specificity == CONTACT_SPECIFICITY_NATIONAL_GENERIC,
            "wrong_school_match": bool(payload.get("reasonCode") == "wrong_school_match"),
        }
        if status == "verified" and job.field_name == FIELD_JOB_VERIFY_SCHOOL:
            business_signals["validated_active"] = True
        if (result and result.chapter_updates.get("chapter_status") == "inactive") or status == "inactive":
            business_signals["validated_inactive"] = True
        if status == "updated" and field_name == "website_url":
            business_signals["chapter_safe_website"] = specificity in safe_specificity
        if status == "updated" and field_name == "contact_email":
            business_signals["chapter_safe_email"] = specificity in safe_specificity
        if status == "updated" and field_name == "instagram_url":
            business_signals["chapter_safe_instagram"] = specificity in safe_specificity
        if status == "updated":
            completion_lookup = getattr(self._repository, "get_chapter_completion_signal", None)
            completion = completion_lookup(job.chapter_id) if callable(completion_lookup) else {}
            business_signals["complete_row"] = bool(completion.get("complete_row", False))
            if not business_signals["validated_active"]:
                business_signals["validated_active"] = bool(completion.get("validated_active", False))
            if not business_signals["chapter_safe_email"]:
                business_signals["chapter_safe_email"] = bool(completion.get("chapter_safe_email", False))
            if not business_signals["chapter_safe_instagram"]:
                business_signals["chapter_safe_instagram"] = bool(completion.get("chapter_safe_instagram", False))
        return {
            "finalState": "processed",
            "status": status,
            "reasonCode": payload.get("reasonCode"),
            "businessSignals": business_signals,
        }

    def _record_enrichment_observation(
        self,
        job: FieldJob,
        *,
        context: dict[str, object],
        decisions: list[dict[str, object]],
        deterministic_action: str,
        outcome: dict[str, object],
    ) -> None:
        if not self._enrichment_observations_enabled:
            return
        append_observation = getattr(self._repository, "append_enrichment_observation", None)
        if not callable(append_observation):
            return
        observation = EnrichmentObservation(
            id=None,
            field_job_id=job.id,
            chapter_id=job.chapter_id,
            chapter_slug=job.chapter_slug,
            fraternity_slug=job.fraternity_slug,
            source_slug=job.source_slug,
            field_name=job.field_name,
            queue_state=job.queue_state,
            runtime_mode=self._adaptive_runtime_mode,
            policy_version=self._adaptive_policy_version,
            policy_mode=self._adaptive_policy_mode,
            recommended_action=str((decisions[0] or {}).get("actionType")) if decisions else None,
            deterministic_action=deterministic_action,
            recommended_actions=decisions,
            context_features=context,
            provider_window_state=dict(self._provider_window_state or {}),
            outcome=outcome,
        )
        append_observation(observation)
        self._enrichment_observations_logged += 1

    def _append_chapter_search_queries(self, summary: dict[str, object], queries: list[str]) -> None:
        bucket = summary.setdefault("chapter_search_queries", [])
        if not isinstance(bucket, list):
            return
        for query in queries:
            query_text = str(query or "").strip()
            if query_text and query_text not in bucket:
                bucket.append(query_text)

    def _accumulate_process_metrics(self, summary: dict[str, object], job: FieldJob, result: FieldJobResult) -> None:
        self._append_chapter_search_queries(summary, self._chapter_search_queries)
        payload = dict(result.completed_payload or {})
        if str(payload.get("status") or "").strip().lower() not in {"updated", "confirmed_absent"}:
            return
        resolution = payload.get("resolutionEvidence") if isinstance(payload.get("resolutionEvidence"), dict) else {}
        decision_stage = str(resolution.get("decisionStage") or "").strip().lower()
        target_field = str(payload.get("field") or FIELD_TO_CHAPTER_COLUMN.get(job.field_name, job.field_name) or "").strip().lower()
        if target_field == "find_email":
            target_field = "contact_email"
        elif target_field == "find_instagram":
            target_field = "instagram_url"
        elif target_field == "find_website":
            target_field = "website_url"
        if decision_stage == "authoritative_bundle":
            if target_field == "website_url":
                summary["supporting_page_resolved"] = int(summary["supporting_page_resolved"]) + 1
            elif target_field in {"contact_email", "instagram_url"}:
                summary["supporting_page_contact_resolved"] = int(summary["supporting_page_contact_resolved"]) + 1
        elif decision_stage == "search_candidate" and target_field in {"contact_email", "instagram_url"}:
            summary["external_search_contact_resolved"] = int(summary["external_search_contact_resolved"]) + 1

    def process_claimed_job(self, job: FieldJob) -> FieldJobResult:
        return self._process_single_job(job)

    def _process_single_job(self, job: FieldJob) -> FieldJobResult:
        self._search_errors_encountered = False
        self._search_queries_attempted = 0
        self._search_queries_failed = 0
        self._search_queries_succeeded = 0
        self._search_fanout_aborted = False
        self._search_skipped_due_to_degraded_mode = False
        self._last_provider_attempts = []
        self._last_query_provider_attempts = []
        self._last_search_failure_kind = None
        self._candidate_rejection_counts = {}
        self._decision_trace = []
        self._chapter_search_queries = []
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

        inactive_result = self._existing_inactive_chapter_result(job)
        if inactive_result is not None:
            return inactive_result

        invalid_entity_result = self._existing_invalid_entity_result(job)
        if invalid_entity_result is not None:
            return invalid_entity_result

        if job.field_name == FIELD_JOB_FIND_EMAIL:
            existing_email = sanitize_as_email(job.contact_email)
            if existing_email and self._existing_email_is_confident(job, existing_email):
                self._trace("already_populated", target="contact_email")
                return self._already_populated_result(job.field_name, existing_email)
            invalid_entity_result = self._resolve_invalid_entity_gate(job, target_field="contact_email")
            if invalid_entity_result is not None:
                return invalid_entity_result
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
            if not _website_is_confident(job):
                validation_result = self._resolve_activity_gate(job, target_field="contact_email")
                if validation_result is not None:
                    return validation_result
                bundle_result = self._authoritative_bundle_result(job, target_field="contact_email")
                if bundle_result is not None:
                    return bundle_result
            match = self._find_email_candidate(job)
            if match is None:
                if self._search_degraded_mode and _job_supporting_page_ready(job):
                    self._trace("terminal_no_signal", target="contact_email", reason="authoritative_supporting_page_no_contact")
                    return self._terminal_no_signal_result(job, "contact_email", reason_code="authoritative_supporting_page_no_contact")
                if self._should_complete_provider_degraded(job):
                    self._trace("provider_degraded", target="contact_email")
                    return self._provider_degraded_result(job, "contact_email")
                self._trace("no_candidate", target="contact_email")
                self._emit_candidate_rejection_summary(job, target="email")
                raise self._no_candidate_error(job, "No candidate email found in provenance, chapter website, or search results")
            self._trace("candidate_selected", target="contact_email", confidence=round(match.confidence, 4), provider=match.source_provider)
            return self._candidate_result(job, match, "contact_email")

        if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
            existing_instagram = sanitize_as_instagram(job.instagram_url)
            if existing_instagram:
                audit_result = self._instagram_audit_result(job, existing_instagram)
                if audit_result is not None:
                    self._trace(
                        "instagram_existing_audited",
                        target="instagram_url",
                        status=str(audit_result.completed_payload.get("status") or ""),
                        reason=str(audit_result.completed_payload.get("reasonCode") or ""),
                    )
                    return audit_result
                if self._existing_instagram_is_confident(job, existing_instagram):
                    self._trace("already_populated", target="instagram_url")
                    return self._already_populated_result(job.field_name, existing_instagram)
            invalid_entity_result = self._resolve_invalid_entity_gate(job, target_field="instagram_url")
            if invalid_entity_result is not None:
                return invalid_entity_result
            admission_result = self._admission_gate(job, "instagram_url")
            if admission_result is not None:
                return admission_result
            if not _website_is_confident(job):
                validation_result = self._resolve_activity_gate(job, target_field="instagram_url")
                if validation_result is not None:
                    return validation_result
                bundle_result = self._authoritative_bundle_result(job, target_field="instagram_url")
                if bundle_result is not None:
                    return bundle_result
            match = self._find_instagram_candidate(job)
            if match is not None:
                self._trace("candidate_selected", target="instagram_url", confidence=round(match.confidence, 4), provider=match.source_provider)
                return self._candidate_result(job, match, "instagram_url")
            fallback_result = self._resolve_instagram_search_miss(job)
            if fallback_result is not None:
                self._trace("inactive_resolution", target="instagram_url")
                return fallback_result
            if self._search_degraded_mode and _job_supporting_page_ready(job):
                self._trace("terminal_no_signal", target="instagram_url", reason="authoritative_supporting_page_no_contact")
                return self._terminal_no_signal_result(job, "instagram_url", reason_code="authoritative_supporting_page_no_contact")
            if self._should_complete_provider_degraded(job):
                self._trace("provider_degraded", target="instagram_url")
                return self._provider_degraded_result(job, "instagram_url")
            self._trace("no_candidate", target="instagram_url")
            self._emit_candidate_rejection_summary(job, target="instagram")
            raise self._no_candidate_error(job, "No candidate instagram URL found in provenance, chapter website, or search results")

        if job.field_name == FIELD_JOB_FIND_WEBSITE:
            existing_website = _current_website_url(job)
            if existing_website and self._existing_website_is_confident(job, existing_website):
                self._trace("already_populated", target="website_url")
                return self._already_populated_result(job.field_name, existing_website)
            invalid_entity_result = self._resolve_invalid_entity_gate(job, target_field="website_url")
            if invalid_entity_result is not None:
                return invalid_entity_result
            admission_result = self._admission_gate(job, "website_url")
            if admission_result is not None:
                return admission_result
            validation_result = self._resolve_activity_gate(job, target_field="website_url")
            if validation_result is not None:
                return validation_result
            bundle_result = self._authoritative_bundle_result(job, target_field="website_url")
            if bundle_result is not None:
                return bundle_result
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
        queue_state = job.queue_state or ((job.payload.get("contactResolution") or {}).get("queueState") if isinstance(job.payload.get("contactResolution"), dict) else None) or "actionable"
        return queue_state in {"deferred", "blocked_provider"} and self._payload_int(job.payload.get("transient_provider_failures")) >= self._transient_short_retries and int(job.attempts or 0) > 1

    def _school_name_for_job(self, job: FieldJob) -> str:
        for candidate in (job.university_name, job.payload.get("candidateSchoolName")):
            normalized = _canonical_school_name(candidate)
            if normalized:
                return normalized
        return ""

    def _school_slug_for_job(self, job: FieldJob) -> str:
        return str(_slugify(self._school_name_for_job(job)) or "")

    def _fraternity_name_for_job(self, job: FieldJob) -> str:
        return _display_name(job.fraternity_slug)

    def _classify_page_scope(
        self,
        job: FieldJob,
        *,
        page_url: str | None,
        page_text: str = "",
        prefer_chapter_nationals: bool = False,
    ) -> str:
        if not page_url:
            return PAGE_SCOPE_UNRELATED
        scope = tool_site_scope_classifier(
            page_url=page_url,
            title="",
            text=page_text[:1600],
            fraternity_name=self._fraternity_name_for_job(job),
            school_name=self._school_name_for_job(job),
            chapter_name=job.chapter_name,
        )
        return _map_scope_classifier_to_page_scope(scope.decision, prefer_chapter_nationals=prefer_chapter_nationals)

    def _resolution_evidence_for_candidate(
        self,
        job: FieldJob,
        match: CandidateMatch,
        *,
        target_field: str,
        decision_stage: str,
    ) -> dict[str, Any]:
        parsed_source = urlparse(match.source_url or "")
        parsed_base = urlparse(job.source_base_url or "")
        parsed_candidate = urlparse(match.value or "") if target_field in {"website_url", "instagram_url"} else urlparse("")
        snippet_text = _normalized_match_text(match.source_snippet)
        supporting_document = SearchDocument(
            text=match.source_snippet,
            url=match.source_url,
            title="",
            provider=match.source_provider,
            query=match.query,
        )
        same_source_host = bool(parsed_source.netloc and parsed_base.netloc) and (
            parsed_source.netloc.lower() == parsed_base.netloc.lower()
            or parsed_source.netloc.lower().endswith(f".{parsed_base.netloc.lower()}")
            or parsed_base.netloc.lower().endswith(f".{parsed_source.netloc.lower()}")
        )
        chapterish_context = any(
            (
                _school_matches(job, snippet_text),
                _fraternity_matches(job, snippet_text),
                _chapter_matches(job, snippet_text),
                bool(match.related_website_url),
                "chapter website" in snippet_text,
                "official student organization" in snippet_text,
            )
        )
        prefer_chapter_nationals = match.source_provider in {"nationals_directory", "chapter_website"} or (
            match.source_provider == "provenance" and same_source_host and chapterish_context
        )
        page_scope = self._classify_page_scope(
            job,
            page_url=match.source_url,
            page_text=match.source_snippet,
            prefer_chapter_nationals=prefer_chapter_nationals,
        )
        if target_field == "website_url":
            trust_tier = _website_trust_tier(job, match.value or "")
            if trust_tier == "tier1":
                page_scope = PAGE_SCOPE_SCHOOL_AFFILIATION
            elif parsed_candidate.netloc and not _website_candidate_looks_low_signal(match.value or ""):
                candidate_same_as_source = bool(parsed_candidate.netloc and parsed_base.netloc) and (
                    parsed_candidate.netloc.lower() == parsed_base.netloc.lower()
                    or parsed_candidate.netloc.lower().endswith(f".{parsed_base.netloc.lower()}")
                    or parsed_base.netloc.lower().endswith(f".{parsed_candidate.netloc.lower()}")
                )
                if not candidate_same_as_source:
                    page_scope = PAGE_SCOPE_CHAPTER_SITE
        if match.source_provider == "nationals_directory":
            page_scope = PAGE_SCOPE_NATIONALS_CHAPTER
        elif match.source_provider == "provenance" and same_source_host:
            if chapterish_context or bool((parsed_source.path or "").strip("/")):
                page_scope = PAGE_SCOPE_NATIONALS_CHAPTER
            else:
                page_scope = PAGE_SCOPE_NATIONALS_GENERIC
        if (
            target_field == "instagram_url"
            and match.source_provider in {"chapter_website", "provenance", "nationals_directory"}
            and page_scope != PAGE_SCOPE_SCHOOL_AFFILIATION
            and sanitize_as_instagram(match.value)
            and _instagram_looks_relevant_to_job(sanitize_as_instagram(match.value) or "", job, document=supporting_document)
        ):
            page_scope = PAGE_SCOPE_CHAPTER_SITE
        elif target_field == "instagram_url" and ("instagram.com" in parsed_source.netloc.lower()) and chapterish_context:
            page_scope = PAGE_SCOPE_CHAPTER_SITE
        elif (
            target_field == "contact_email"
            and match.source_provider in {"chapter_website", "provenance", "nationals_directory"}
            and page_scope != PAGE_SCOPE_SCHOOL_AFFILIATION
            and sanitize_as_email(match.value)
            and _email_looks_relevant_to_job(sanitize_as_email(match.value) or "", job, document=supporting_document)
        ):
            page_scope = PAGE_SCOPE_CHAPTER_SITE
        if target_field == "website_url" and parsed_candidate.netloc and not _website_candidate_looks_low_signal(match.value or ""):
            candidate_same_as_source = bool(parsed_candidate.netloc and parsed_base.netloc) and (
                parsed_candidate.netloc.lower() == parsed_base.netloc.lower()
                or parsed_candidate.netloc.lower().endswith(f".{parsed_base.netloc.lower()}")
                or parsed_base.netloc.lower().endswith(f".{parsed_candidate.netloc.lower()}")
            )
            if not candidate_same_as_source:
                page_scope = PAGE_SCOPE_CHAPTER_SITE
        status_decision = self._status_decision_cache.get(job.chapter_id)
        return {
            "decisionStage": decision_stage,
            "evidenceUrl": match.source_url,
            "sourceType": match.source_provider,
            "pageScope": page_scope,
            "contactSpecificity": _contact_specificity_for_page_scope(page_scope),
            "confidence": round(match.confidence, 4),
            "reasonCode": None,
            "metadata": {
                "query": match.query,
                "relatedWebsiteUrl": match.related_website_url,
                **(status_decision_metadata(status_decision) if status_decision is not None else {}),
            },
        }

    def _resolution_evidence_for_activity_decision(self, decision: ActivityValidationDecision, *, decision_stage: str) -> dict[str, Any]:
        source_type = str(decision.evidence_source_type or "official_school").strip() or "official_school"
        page_scope = PAGE_SCOPE_SCHOOL_AFFILIATION if "school" in source_type else PAGE_SCOPE_NATIONALS_CHAPTER
        return {
            "decisionStage": decision_stage,
            "evidenceUrl": decision.evidence_url,
            "sourceType": source_type,
            "pageScope": page_scope,
            "contactSpecificity": _contact_specificity_for_page_scope(page_scope),
            "confidence": round(float(decision.confidence or 0.0), 4),
            "reasonCode": decision.reason_code,
            "metadata": {
                **dict(decision.metadata or {}),
                **(
                    {
                        "statusDecisionId": decision.status_decision_id,
                        "finalStatus": decision.final_status,
                        "schoolRecognitionStatus": decision.school_recognition_status,
                        "nationalStatus": decision.national_status,
                        "reviewRequired": decision.review_required,
                    }
                    if decision.status_decision_id
                    else {}
                ),
            },
        }

    def _resolution_evidence_for_authoritative_bundle(
        self,
        job: FieldJob,
        bundle: AuthoritativeBundle,
        *,
        target_field: str,
    ) -> dict[str, Any]:
        match = {
            "website_url": bundle.website_match,
            "contact_email": bundle.email_match,
            "instagram_url": bundle.instagram_match,
        }.get(target_field)
        if match is not None:
            return self._resolution_evidence_for_candidate(job, match, target_field=target_field, decision_stage="authoritative_bundle")
        source_type = str(bundle.evidence_source_type or "authoritative_validation").strip() or "authoritative_validation"
        page_scope = PAGE_SCOPE_SCHOOL_AFFILIATION if "school" in source_type else PAGE_SCOPE_NATIONALS_CHAPTER if "national" in source_type else PAGE_SCOPE_DIRECTORY
        status_decision = self._status_decision_cache.get(job.chapter_id)
        return {
            "decisionStage": "authoritative_bundle",
            "evidenceUrl": bundle.evidence_url,
            "sourceType": source_type,
            "pageScope": page_scope,
            "contactSpecificity": _contact_specificity_for_page_scope(page_scope),
            "confidence": 0.9,
            "reasonCode": bundle.reason_code,
            "metadata": status_decision_metadata(status_decision) if status_decision is not None else {},
        }

    def _decision_from_school_policy(self, record) -> ActivityValidationDecision:
        school_policy_status = str(getattr(record, "greek_life_status", "unknown") or "unknown")
        return ActivityValidationDecision(
            school_policy_status=school_policy_status,
            final_status="inactive" if school_policy_status == "banned" else "active" if school_policy_status == "allowed" else "unknown",
            school_recognition_status="banned_no_greek_life" if school_policy_status == "banned" else "recognized" if school_policy_status == "allowed" else "unknown",
            evidence_url=getattr(record, "evidence_url", None),
            evidence_source_type=getattr(record, "evidence_source_type", None),
            reason_code=getattr(record, "reason_code", None),
            source_snippet=str((getattr(record, "metadata", {}) or {}).get("sourceSnippet") or "")[:400] or None,
            confidence=float(getattr(record, "confidence", 0.0) or 0.0),
            metadata=dict(getattr(record, "metadata", {}) or {}),
        )

    def _decision_from_chapter_activity(self, record) -> ActivityValidationDecision:
        return ActivityValidationDecision(
            chapter_activity_status=str(getattr(record, "chapter_activity_status", "unknown") or "unknown"),
            final_status="active" if str(getattr(record, "chapter_activity_status", "unknown") or "unknown") == "confirmed_active" else "inactive" if str(getattr(record, "chapter_activity_status", "unknown") or "unknown") == "confirmed_inactive" else "unknown",
            school_recognition_status="recognized" if str(getattr(record, "chapter_activity_status", "unknown") or "unknown") == "confirmed_active" else "unrecognized" if str(getattr(record, "chapter_activity_status", "unknown") or "unknown") == "confirmed_inactive" else "unknown",
            evidence_url=getattr(record, "evidence_url", None),
            evidence_source_type=getattr(record, "evidence_source_type", None),
            reason_code=getattr(record, "reason_code", None),
            source_snippet=str((getattr(record, "metadata", {}) or {}).get("sourceSnippet") or "")[:400] or None,
            confidence=float(getattr(record, "confidence", 0.0) or 0.0),
            metadata=dict(getattr(record, "metadata", {}) or {}),
        )

    def _activity_decision_from_status_decision(
        self,
        status_decision: ChapterStatusDecision,
        *,
        evidence_url: str | None = None,
        evidence_source_type: str = "official_school",
        source_snippet: str | None = None,
    ) -> ActivityValidationDecision:
        activity_status = chapter_activity_status_from_decision(status_decision)
        school_policy_status = school_policy_status_from_decision(status_decision)
        return ActivityValidationDecision(
            school_policy_status=school_policy_status,
            chapter_activity_status=activity_status,
            final_status=str(status_decision.final_status),
            school_recognition_status=str(status_decision.school_recognition_status),
            national_status=str(status_decision.national_status),
            evidence_url=evidence_url,
            evidence_source_type=evidence_source_type,
            reason_code=status_decision.reason_code,
            source_snippet=source_snippet,
            confidence=float(status_decision.confidence),
            status_decision_id=status_decision.id,
            review_required=bool(status_decision.review_required),
            metadata=status_decision_metadata(status_decision),
        )

    def _persist_status_decision(
        self,
        job: FieldJob,
        *,
        status_decision: ChapterStatusDecision,
        campus_index,
        evidence_url: str | None,
        source_snippet: str | None,
    ) -> ChapterStatusDecision:
        if hasattr(self._repository, "upsert_campus_status_source"):
            for source in campus_index.sources:
                source_id = self._repository.upsert_campus_status_source(source)
                zones = [zone for zone in campus_index.zones if zone.source_url == source.source_url]
                if hasattr(self._repository, "replace_campus_status_zones"):
                    self._repository.replace_campus_status_zones(campus_status_source_id=source_id, zones=zones)

        persisted_evidence_ids: list[str] = []
        if hasattr(self._repository, "insert_chapter_status_evidence"):
            source_urls = [
                value
                for value in [
                    status_decision.decision_trace.get("winning_evidence_id"),
                    *list(status_decision.decision_trace.get("conflicting_evidence_ids") or []),
                ]
                if isinstance(value, str) and value.strip()
            ]
            if not source_urls:
                source_urls = [source.source_url for source in campus_index.sources[:1]]
            if not source_urls and evidence_url:
                source_urls = [evidence_url]
            if not source_urls:
                source_urls = [f"status://{job.chapter_id}"]
            for source_url in source_urls[:4]:
                persisted_evidence_ids.append(
                    self._repository.insert_chapter_status_evidence(
                        ChapterStatusEvidence(
                            chapter_id=job.chapter_id,
                            fraternity_name=self._fraternity_name_for_job(job),
                            school_name=self._school_name_for_job(job),
                            source_url=source_url,
                            authority_tier=1,
                            evidence_type="official_school_status_zone" if "school" in (source_url or "") or source_url in {source.source_url for source in campus_index.sources} else "national_directory_status",
                            status_signal=status_decision.reason_code,
                            matched_text=source_snippet,
                            zone_type=str(status_decision.school_recognition_status),
                            match_confidence=float(status_decision.confidence),
                            evidence_confidence=float(status_decision.confidence),
                            metadata={
                                "statusDecisionBasis": status_decision.reason_code,
                                "conflictFlags": list(status_decision.conflict_flags),
                            },
                        )
                    )
                )
        final_decision = status_decision.model_copy(update={"evidence_ids": persisted_evidence_ids or status_decision.evidence_ids})
        if hasattr(self._repository, "insert_chapter_status_decision"):
            final_decision = self._repository.insert_chapter_status_decision(chapter_id=job.chapter_id, decision=final_decision)
        school_policy_status = school_policy_status_from_decision(final_decision)
        if school_policy_status in {"allowed", "banned"} and hasattr(self._repository, "upsert_school_policy"):
            self._repository.upsert_school_policy(
                school_name=self._school_name_for_job(job),
                greek_life_status=school_policy_status,
                confidence=float(final_decision.confidence),
                evidence_url=evidence_url,
                evidence_source_type="official_school",
                reason_code=final_decision.reason_code,
                metadata={"statusDecisionId": final_decision.id},
            )
        chapter_activity_status = chapter_activity_status_from_decision(final_decision)
        if chapter_activity_status in {"confirmed_active", "confirmed_inactive"} and hasattr(self._repository, "upsert_chapter_activity"):
            self._repository.upsert_chapter_activity(
                fraternity_slug=str(job.fraternity_slug or ""),
                school_name=self._school_name_for_job(job),
                chapter_activity_status=chapter_activity_status,
                confidence=float(final_decision.confidence),
                evidence_url=evidence_url,
                evidence_source_type="official_school",
                reason_code=final_decision.reason_code,
                metadata={"statusDecisionId": final_decision.id},
            )
        self._status_decision_cache[job.chapter_id] = final_decision
        return final_decision

    def _build_status_decision(self, job: FieldJob) -> ChapterStatusDecision | None:
        school_name = self._school_name_for_job(job)
        fraternity_name = self._fraternity_name_for_job(job)
        if not school_name or not fraternity_name:
            return None

        school_policy = self._get_or_resolve_school_policy(job)
        if school_policy.school_policy_status == "banned":
            decision = ChapterStatusDecision(
                final_status=ChapterStatusFinal.INACTIVE,
                school_recognition_status=SchoolRecognitionStatus.BANNED_NO_GREEK_LIFE,
                national_status="unknown",
                reason_code=school_policy.reason_code or "official_school_policy_prohibits_fraternities",
                confidence=float(school_policy.confidence or 0.0),
                evidence_ids=[school_policy.evidence_url or "school-policy"],
                decision_trace={"authority_order": ["school_policy"], "winning_evidence_id": school_policy.evidence_url},
            )
            return self._persist_status_decision(
                job,
                status_decision=decision,
                campus_index=build_campus_status_index(school_name=school_name, documents=[]),
                evidence_url=school_policy.evidence_url,
                source_snippet=school_policy.source_snippet,
            )

        chapter_activity = self._get_or_resolve_chapter_activity(job)
        if chapter_activity.chapter_activity_status == "confirmed_active":
            decision = ChapterStatusDecision(
                final_status=ChapterStatusFinal.ACTIVE,
                school_recognition_status=SchoolRecognitionStatus.RECOGNIZED,
                national_status="unknown",
                reason_code=chapter_activity.reason_code or "official_school_current_recognition",
                confidence=float(chapter_activity.confidence or 0.0),
                evidence_ids=[chapter_activity.evidence_url or "chapter-activity"],
                decision_trace={"authority_order": ["school_status"], "winning_evidence_id": chapter_activity.evidence_url},
            )
            return self._persist_status_decision(
                job,
                status_decision=decision,
                campus_index=build_campus_status_index(school_name=school_name, documents=[]),
                evidence_url=chapter_activity.evidence_url,
                source_snippet=chapter_activity.source_snippet,
            )

        if chapter_activity.chapter_activity_status == "confirmed_inactive":
            decision = ChapterStatusDecision(
                final_status=ChapterStatusFinal.INACTIVE,
                school_recognition_status=SchoolRecognitionStatus.UNRECOGNIZED,
                national_status="unknown",
                reason_code=chapter_activity.reason_code or "official_school_negative_status",
                confidence=float(chapter_activity.confidence or 0.0),
                evidence_ids=[chapter_activity.evidence_url or "chapter-activity"],
                decision_trace={"authority_order": ["school_status"], "winning_evidence_id": chapter_activity.evidence_url},
            )
            return self._persist_status_decision(
                job,
                status_decision=decision,
                campus_index=build_campus_status_index(school_name=school_name, documents=[]),
                evidence_url=chapter_activity.evidence_url,
                source_snippet=chapter_activity.source_snippet,
            )

        documents: list[CampusSourceDocument] = []
        seen_urls: set[str] = set()

        reusable_url = None
        if hasattr(self._repository, "get_reusable_official_school_evidence_url"):
            reusable_url = self._repository.get_reusable_official_school_evidence_url(
                fraternity_slug=job.fraternity_slug,
                school_name=school_name,
            )
        if reusable_url:
            reusable_document = self._fetch_search_document(reusable_url, provider="official_school_cache")
            if reusable_document is not None and reusable_document.url:
                seen_urls.add(_normalize_url(reusable_document.url))
                documents.append(
                    CampusSourceDocument(
                        page_url=reusable_document.url,
                        title=reusable_document.title or "",
                        text=reusable_document.text,
                        html=reusable_document.html or "",
                    )
                )

        for target_name, query_limit, page_limit in (
            ("school_chapter_list", 4, 3),
            ("website_school", 2, 2),
            ("campus_policy", 2, 2),
        ):
            for document in self._build_validation_documents(
                job,
                target=target_name,
                query_limit=query_limit,
                page_limit=page_limit,
                require_official_school=True,
            ):
                normalized = _normalize_url(document.url or f"{target_name}:{document.title}:{document.query}")
                if normalized in seen_urls or not document.url:
                    continue
                seen_urls.add(normalized)
                documents.append(
                    CampusSourceDocument(
                        page_url=document.url,
                        title=document.title or "",
                        text=document.text,
                        html=document.html or "",
                    )
                )

        if not documents:
            return None

        campus_index = build_campus_status_index(school_name=school_name, documents=documents)

        national_evidence = None
        source_record = self._load_source_record(job.source_slug) if job.source_slug else None
        source_list_url = _source_list_url_for_job(job, source_record)
        if source_list_url:
            source_document = self._fetch_search_document(source_list_url, provider="nationals_directory")
            if source_document is not None and source_document.url:
                national_evidence = infer_national_status_from_page(
                    fraternity_name=fraternity_name,
                    school_name=school_name,
                    page_url=source_document.url,
                    title=source_document.title or "",
                    text=source_document.text,
                    html=source_document.html or "",
                )

        decision = decide_chapter_status(
            fraternity_name=fraternity_name,
            fraternity_slug=job.fraternity_slug,
            school_name=school_name,
            index=campus_index,
            national_evidence=national_evidence,
        )
        evidence_url = None
        if campus_index.sources:
            evidence_url = campus_index.sources[0].source_url
        source_snippet = documents[0].text[:400] if documents else None
        return self._persist_status_decision(
            job,
            status_decision=decision,
            campus_index=campus_index,
            evidence_url=evidence_url,
            source_snippet=source_snippet,
        )

    def _get_or_resolve_status_decision(self, job: FieldJob) -> ChapterStatusDecision | None:
        cached = self._status_decision_cache.get(job.chapter_id)
        if cached is not None:
            return cached
        decision = None
        if hasattr(self._repository, "get_latest_chapter_status_decision"):
            decision = self._repository.get_latest_chapter_status_decision(job.chapter_id)
        if decision is not None:
            self._status_decision_cache[job.chapter_id] = decision
            return decision
        decision = self._build_status_decision(job)
        if decision is not None:
            self._status_decision_cache[job.chapter_id] = decision
        return decision

    def _existing_inactive_chapter_result(self, job: FieldJob) -> FieldJobResult | None:
        state_key = FIELD_JOB_TO_STATE_KEY.get(job.field_name)
        if not state_key:
            return None
        chapter_status = str(getattr(job, "chapter_status", "active") or "active").strip().lower()
        target_state = str((job.field_states or {}).get(state_key) or "").strip().lower()
        all_contact_states = {
            key: str((job.field_states or {}).get(key) or "").strip().lower()
            for key in _STATE_KEY_TO_FIELD_JOB
        }
        chapter_inactive = chapter_status == "inactive" or target_state == "inactive" or (
            all_contact_states and all(state == "inactive" for state in all_contact_states.values())
        )
        if not chapter_inactive:
            return None
        sibling_field_names = [
            field_name
            for candidate_state_key, field_name in _STATE_KEY_TO_FIELD_JOB.items()
            if candidate_state_key != state_key
        ]
        self._repository.apply_chapter_inactive_status(
            chapter_id=job.chapter_id,
            chapter_slug=job.chapter_slug,
            fraternity_slug=job.fraternity_slug,
            source_slug=job.source_slug,
            crawl_run_id=job.crawl_run_id,
            reason_code="chapter_already_inactive",
            evidence_url=None,
            evidence_source_type="system",
            source_snippet=None,
            provider="field_job_validation",
            metadata={"revalidated": True},
        )
        self._repository.complete_pending_field_jobs_for_chapter(
            chapter_id=job.chapter_id,
            reason_code="chapter_already_inactive",
            status="inactive_by_school_validation",
            field_states={candidate_state_key: "inactive" for candidate_state_key in _STATE_KEY_TO_FIELD_JOB if candidate_state_key != state_key},
            field_names=sibling_field_names,
        )
        self._trace("chapter_status_gate", status="inactive", target=state_key)
        return FieldJobResult(
            chapter_updates={},
            completed_payload={
                "status": "inactive_by_school_validation",
                "field": state_key,
                "reasonCode": "chapter_already_inactive",
                "decision_trace": self._build_decision_trace_summary(),
            },
            field_state_updates={state_key: "inactive"},
        )

    def _latest_provenance_context_for_job(self, job: FieldJob) -> dict[str, Any]:
        cached = self._latest_provenance_context_cache.get(job.chapter_id)
        if cached is not None:
            return cached
        fetcher = getattr(self._repository, "fetch_latest_provenance_context", None)
        context: dict[str, Any] | None = None
        if callable(fetcher):
            raw = fetcher(job.chapter_id)
            if isinstance(raw, dict):
                context = dict(raw)
        if context is None:
            snippets = self._repository.fetch_provenance_snippets(job.chapter_id)
            context = {
                "source_url": None,
                "source_snippet": snippets[0] if snippets else None,
                "field_name": None,
                "confidence": 0.0,
            }
        self._latest_provenance_context_cache[job.chapter_id] = context
        return context

    def _existing_invalid_entity_result(self, job: FieldJob) -> FieldJobResult | None:
        state_key = FIELD_JOB_TO_STATE_KEY.get(job.field_name)
        if not state_key:
            return None
        target_state = str((job.field_states or {}).get(state_key) or "").strip().lower()
        all_contact_states = {
            key: str((job.field_states or {}).get(key) or "").strip().lower()
            for key in _STATE_KEY_TO_FIELD_JOB
        }
        chapter_invalid = target_state == "invalid_entity" or (
            all_contact_states and all(state == "invalid_entity" for state in all_contact_states.values())
        )
        if not chapter_invalid:
            return None
        sibling_field_names = [
            field_name
            for candidate_state_key, field_name in _STATE_KEY_TO_FIELD_JOB.items()
            if candidate_state_key != state_key
        ]
        self._repository.complete_pending_field_jobs_for_chapter(
            chapter_id=job.chapter_id,
            reason_code="invalid_entity_cached",
            status="invalid_entity_filtered",
            field_states={candidate_state_key: "invalid_entity" for candidate_state_key in _STATE_KEY_TO_FIELD_JOB if candidate_state_key != state_key},
            field_names=sibling_field_names,
        )
        self._trace("invalid_entity_gate", status="cached_invalid_entity", target=state_key)
        return FieldJobResult(
            chapter_updates={},
            completed_payload={
                "status": "invalid_entity_filtered",
                "field": state_key,
                "reasonCode": "invalid_entity_cached",
                "decision_trace": self._build_decision_trace_summary(),
            },
            field_state_updates={state_key: "invalid_entity"},
        )

    def _mark_invalid_entity(
        self,
        job: FieldJob,
        *,
        target_field: str,
        reason_code: str,
        evidence_url: str | None,
        source_snippet: str | None,
    ) -> FieldJobResult:
        sibling_field_names = [
            field_name
            for state_key, field_name in _STATE_KEY_TO_FIELD_JOB.items()
            if state_key != target_field
        ]
        self._repository.complete_pending_field_jobs_for_chapter(
            chapter_id=job.chapter_id,
            reason_code=reason_code,
            status="invalid_entity_filtered",
            field_states={state_key: "invalid_entity" for state_key in _STATE_KEY_TO_FIELD_JOB if state_key != target_field},
            field_names=sibling_field_names,
        )
        return FieldJobResult(
            chapter_updates={},
            completed_payload={
                "status": "invalid_entity_filtered",
                "field": target_field,
                "reasonCode": reason_code,
                "evidenceUrl": evidence_url,
                "sourceSnippet": source_snippet,
                "decision_trace": self._build_decision_trace_summary(),
            },
            field_state_updates={target_field: "invalid_entity"},
        )

    def _resolve_invalid_entity_gate(self, job: FieldJob, *, target_field: str) -> FieldJobResult | None:
        provenance = self._latest_provenance_context_for_job(job)
        source_url = str(
            provenance.get("source_url")
            or job.payload.get("sourceUrl")
            or job.payload.get("sourceListUrl")
            or job.source_base_url
            or ""
        ).strip()
        source_snippet = str(provenance.get("source_snippet") or "").strip() or None
        confidence = float(provenance.get("confidence") or 0.0)
        decision = classify_chapter_validity(
            ExtractedChapter(
                name=job.chapter_name or "",
                university_name=self._school_name_for_job(job) or None,
                source_url=source_url,
                source_snippet=source_snippet,
                source_confidence=confidence or 0.72,
            ),
            source_class="national",
            provenance="field_job_validation",
        )
        if decision.validity_class != "invalid_non_chapter":
            return None
        reason_code = str(decision.invalid_reason or "invalid_non_chapter").strip() or "invalid_non_chapter"
        self._trace("invalid_entity_gate", status="invalid_non_chapter", reason=reason_code, source_url=source_url or None)
        return self._mark_invalid_entity(
            job,
            target_field=target_field,
            reason_code=reason_code,
            evidence_url=source_url or None,
            source_snippet=source_snippet,
        )

    def _existing_email_is_confident(self, job: FieldJob, email: str) -> bool:
        if not _field_value_is_confident(job, "contact_email"):
            return False
        return _email_looks_relevant_to_job(email, job)

    def _existing_instagram_is_confident(self, job: FieldJob, instagram_url: str) -> bool:
        if not _field_value_is_confident(job, "instagram_url"):
            return False
        if _instagram_has_generic_handle(instagram_url, job):
            return False
        return _instagram_looks_relevant_to_job(instagram_url, job)

    def _existing_website_is_confident(self, job: FieldJob, website_url: str) -> bool:
        if not _field_value_is_confident(job, "website_url"):
            return False
        normalized_url = _normalize_url(website_url)
        if normalized_url.endswith(_DOCUMENT_URL_EXTENSIONS):
            return False
        parsed = urlparse(website_url)
        path_text = _normalized_match_text(f"{parsed.netloc} {parsed.path} {parsed.query}")
        if any(marker in path_text for marker in ("archive", "archives", "download", "digital")):
            return False
        if _candidate_is_source_domain(website_url, job):
            return False
        if _website_candidate_looks_low_signal(website_url):
            return False

        document = self._fetch_search_document(website_url, provider="chapter_website")
        if document is None:
            return True

        scope = tool_site_scope_classifier(
            page_url=website_url,
            title=document.title or "",
            text=document.text[:1600],
            fraternity_name=self._fraternity_name_for_job(job),
            school_name=self._school_name_for_job(job),
            chapter_name=job.chapter_name,
        )
        if scope.decision in {"nationals", "school_affiliation"}:
            return False
        combined = _document_match_text(document, limit=1200)
        if _school_has_conflicting_signal(job, combined):
            return False
        if _website_document_has_conflicting_org_signal(job, document):
            return False
        if scope.decision == "chapter_site":
            return True

        return _website_trust_tier(job, website_url) == "unknown"

    def _build_validation_documents(
        self,
        job: FieldJob,
        *,
        target: str,
        query_limit: int,
        page_limit: int,
        require_official_school: bool = False,
    ) -> list[SearchDocument]:
        if self._search_degraded_mode:
            self._trace("validation_search_skipped", target=target, reason="preflight_degraded")
            return []
        if self._provider_search_hard_blocked():
            self._trace("validation_search_skipped", target=target, reason="provider_unavailable")
            return []
        documents: list[SearchDocument] = []
        seen_urls: set[str] = set()
        fetched_pages = 0
        school_name = self._school_name_for_job(job)
        for query in self._build_search_queries(job, target)[:query_limit]:
            query_results = self._run_search(query)
            if self._maybe_abort_search_sequence(job, target=target, query_results=query_results):
                break
            for result in query_results[:6]:
                normalized_url = _normalize_url(result.url)
                if normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)
                host = (urlparse(result.url).netloc or "").lower()
                is_official_school = bool(host) and (_website_trust_tier(job, result.url) == "tier1" or any(host == domain or host.endswith(f".{domain}") for domain in _campus_domains(job)))
                if require_official_school and not is_official_school:
                    continue
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
                if fetched_pages >= page_limit or _should_skip_search_page_fetch(result.url):
                    continue
                if require_official_school and not is_official_school:
                    continue
                fetched = self._fetch_search_document(result.url, provider="search_page", query=query)
                if fetched is None:
                    continue
                follow_urls: list[str] = []
                if target in {"school_chapter_list", "website_school"}:
                    follow_urls = self._school_validation_follow_links(fetched)
                if require_official_school:
                    scoped = tool_site_scope_classifier(
                        page_url=fetched.url or result.url,
                        title=fetched.title or result.title,
                        text=fetched.text,
                        fraternity_name=self._fraternity_name_for_job(job),
                        school_name=school_name,
                        chapter_name=job.chapter_name,
                    )
                    if scoped.decision in {"school_affiliation"}:
                        documents.append(fetched)
                        fetched_pages += 1
                else:
                    documents.append(fetched)
                    fetched_pages += 1
                if target in {"school_chapter_list", "website_school"}:
                    for follow_url in follow_urls:
                        normalized_follow_url = _normalize_url(follow_url)
                        if normalized_follow_url in seen_urls or fetched_pages >= page_limit:
                            continue
                        seen_urls.add(normalized_follow_url)
                        follow_document = self._fetch_search_document(follow_url, provider="search_page", query=query)
                        if follow_document is None:
                            continue
                        follow_scope = tool_site_scope_classifier(
                            page_url=follow_document.url or follow_url,
                            title=follow_document.title or "",
                            text=follow_document.text,
                            fraternity_name=self._fraternity_name_for_job(job),
                            school_name=school_name,
                            chapter_name=job.chapter_name,
                        )
                        if follow_scope.decision != "school_affiliation":
                            continue
                        documents.append(follow_document)
                        fetched_pages += 1
        return documents

    def _school_validation_follow_links(self, document: SearchDocument) -> list[str]:
        if not document.html or not document.url:
            return []
        base_host = (urlparse(document.url).netloc or "").lower()
        if not base_host:
            return []
        soup = _parse_document_markup(document.html)
        candidates: list[tuple[int, str]] = []
        seen: set[str] = set()
        for anchor in soup.select("a[href]"):
            href = str(anchor.get("href") or "").strip()
            if not href:
                continue
            resolved = urljoin(document.url, href)
            host = (urlparse(resolved).netloc or "").lower()
            if not host or host != base_host:
                continue
            normalized = _normalize_url(resolved)
            if normalized in seen:
                continue
            text = anchor.get_text(" ", strip=True)
            combined = _normalized_match_text(f"{text} {resolved}")
            if any(marker in combined for marker in ("contact us", "about us", "apply", "visit", "give", "search", "menu")):
                continue
            score = 0
            if "community scorecard" in combined or "chapter scorecards" in combined:
                score += 6
            if "scorecard" in combined:
                score += 4
            if "chapters at" in combined or "chapter list" in combined or "recognized chapters" in combined:
                score += 4
            if "fraternity chapters" in combined:
                score += 4
            if "fraternities" in combined:
                score += 3
            if "chapters" in combined:
                score += 2
            if "interfraternity" in combined or "ifc" in combined:
                score += 2
            if "suspended" in combined or "closed" in combined:
                score -= 1
            if score <= 0:
                continue
            seen.add(normalized)
            candidates.append((score, resolved))
        candidates.sort(key=lambda item: (-item[0], item[1]))
        return [url for _, url in candidates[:3]]

    def _get_or_resolve_school_policy(self, job: FieldJob) -> ActivityValidationDecision:
        school_name = self._school_name_for_job(job)
        school_slug = self._school_slug_for_job(job)
        if not school_name or not school_slug:
            return ActivityValidationDecision()
        cached = self._school_policy_cache.get(school_slug)
        if cached is not None:
            return cached
        stored = self._repository.get_school_policy(school_name)
        if stored is not None:
            stored_status = str(stored.greek_life_status or "unknown").strip().lower()
            stored_source_type = str(stored.evidence_source_type or "").strip().lower()
            stored_reason_code = str(stored.reason_code or "").strip().lower()
            stored_evidence_url = str(stored.evidence_url or "").strip()
            stored_unknown_is_reusable = (
                stored_status == "unknown"
                and stored_source_type == "official_school"
                and bool(stored_evidence_url)
                and stored_reason_code not in {"non_official_school_source", "no_official_school_policy_source_found"}
            )
            if stored_status != "unknown" or stored_unknown_is_reusable:
                self._verify_school_cache_hit_count += 1
                if stored_evidence_url:
                    self._verify_school_official_url_reused_count += 1
                if stored_unknown_is_reusable:
                    reusable_document = self._fetch_search_document(stored_evidence_url, provider="official_school_cache")
                    if reusable_document is not None and reusable_document.url:
                        reusable_decision = tool_campus_greek_life_policy(
                            school_name=school_name,
                            page_url=reusable_document.url,
                            title=reusable_document.title or "",
                            text=reusable_document.text,
                        )
                        if reusable_decision.decision in {"allowed", "banned"}:
                            record = self._repository.upsert_school_policy(
                                school_name=school_name,
                                greek_life_status="banned" if reusable_decision.decision == "banned" else "allowed",
                                confidence=reusable_decision.confidence,
                                evidence_url=reusable_document.url,
                                evidence_source_type="official_school",
                                reason_code=(reusable_decision.reason_codes or [f"school_policy_{reusable_decision.decision}"])[0],
                                metadata={**reusable_decision.as_dict(), "sourceSnippet": reusable_document.text[:400]},
                            )
                            decision = self._decision_from_school_policy(record)
                            self._school_policy_cache[school_slug] = decision
                            return decision
                decision = self._decision_from_school_policy(stored)
                self._school_policy_cache[school_slug] = decision
                return decision

        best_decision = ActivityValidationDecision()
        official_unknown: ActivityValidationDecision | None = None
        self._verify_school_provider_search_attempted_count += 1
        for document in self._build_validation_documents(
            job,
            target="campus_policy",
            query_limit=3,
            page_limit=3,
            require_official_school=True,
        ):
            if not document.url:
                continue
            decision = tool_campus_greek_life_policy(
                school_name=school_name,
                page_url=document.url,
                title=document.title or "",
                text=document.text,
            )
            if decision.decision == "banned":
                best_decision = ActivityValidationDecision(
                    school_policy_status="banned",
                    evidence_url=document.url,
                    evidence_source_type="official_school",
                    reason_code=(decision.reason_codes or ["school_policy_banned"])[0],
                    source_snippet=document.text[:400],
                    confidence=decision.confidence,
                    metadata=decision.as_dict(),
                )
                break
            if decision.decision == "allowed" and best_decision.school_policy_status == "unknown":
                best_decision = ActivityValidationDecision(
                    school_policy_status="allowed",
                    evidence_url=document.url,
                    evidence_source_type="official_school",
                    reason_code=(decision.reason_codes or ["school_policy_allowed"])[0],
                    source_snippet=document.text[:400],
                    confidence=decision.confidence,
                    metadata=decision.as_dict(),
                )
            elif decision.decision == "unknown" and official_unknown is None:
                official_unknown = ActivityValidationDecision(
                    school_policy_status="unknown",
                    evidence_url=document.url,
                    evidence_source_type="official_school",
                    reason_code=(decision.reason_codes or ["school_policy_unknown"])[0],
                    source_snippet=document.text[:400],
                    confidence=decision.confidence,
                    metadata=decision.as_dict(),
                )

        persisted = best_decision if best_decision.school_policy_status != "unknown" else official_unknown
        if persisted is not None and persisted.evidence_source_type == "official_school":
            record = self._repository.upsert_school_policy(
                school_name=school_name,
                greek_life_status=persisted.school_policy_status,
                confidence=persisted.confidence,
                evidence_url=persisted.evidence_url,
                evidence_source_type=persisted.evidence_source_type,
                reason_code=persisted.reason_code,
                metadata={**persisted.metadata, "sourceSnippet": persisted.source_snippet},
            )
            decision = self._decision_from_school_policy(record)
            self._school_policy_cache[school_slug] = decision
            return decision

        decision = ActivityValidationDecision(
            school_policy_status="unknown",
            evidence_url=official_unknown.evidence_url if official_unknown is not None else None,
            evidence_source_type=official_unknown.evidence_source_type if official_unknown is not None else None,
            reason_code=official_unknown.reason_code if official_unknown is not None else "no_official_school_policy_source_found",
            source_snippet=official_unknown.source_snippet if official_unknown is not None else None,
            confidence=official_unknown.confidence if official_unknown is not None else 0.0,
            metadata={
                **(official_unknown.metadata if official_unknown is not None else {}),
                "cacheScope": "process",
            },
        )
        self._school_policy_cache[school_slug] = decision
        return decision

    def _get_or_resolve_chapter_activity(self, job: FieldJob) -> ActivityValidationDecision:
        school_name = self._school_name_for_job(job)
        school_slug = self._school_slug_for_job(job)
        fraternity_slug = str(job.fraternity_slug or "").strip()
        if not school_name or not school_slug or not fraternity_slug:
            return ActivityValidationDecision()
        cache_key = (fraternity_slug, school_slug)
        cached = self._chapter_activity_cache.get(cache_key)
        if cached is not None:
            return cached
        stored = self._repository.get_chapter_activity(fraternity_slug=fraternity_slug, school_name=school_name)
        if stored is not None:
            self._verify_school_cache_hit_count += 1
            stored_evidence_url = str(stored.evidence_url or "").strip()
            if stored_evidence_url:
                self._verify_school_official_url_reused_count += 1
            stored_status = str(stored.chapter_activity_status or "unknown").strip().lower()
            stored_source_type = str(stored.evidence_source_type or "").strip().lower()
            if stored_status == "unknown" and stored_source_type == "official_school" and stored_evidence_url:
                reusable_document = self._fetch_search_document(stored_evidence_url, provider="official_school_cache")
                if reusable_document is not None and reusable_document.url:
                    reusable_decision = tool_school_chapter_list_validator(
                        school_name=school_name,
                        fraternity_name=self._fraternity_name_for_job(job),
                        fraternity_slug=fraternity_slug,
                        page_url=reusable_document.url,
                        title=reusable_document.title or "",
                        text=reusable_document.text,
                        html=reusable_document.html or "",
                    )
                    if reusable_decision.decision in {"confirmed_active", "confirmed_inactive"}:
                        record = self._repository.upsert_chapter_activity(
                            fraternity_slug=fraternity_slug,
                            school_name=school_name,
                            chapter_activity_status=reusable_decision.decision,
                            confidence=reusable_decision.confidence,
                            evidence_url=reusable_document.url,
                            evidence_source_type="official_school",
                            reason_code=(reusable_decision.reason_codes or [reusable_decision.decision])[0],
                            metadata={**reusable_decision.as_dict(), "sourceSnippet": reusable_document.text[:400]},
                        )
                        decision = self._decision_from_chapter_activity(record)
                        self._chapter_activity_cache[cache_key] = decision
                        return decision
            decision = self._decision_from_chapter_activity(stored)
            self._chapter_activity_cache[cache_key] = decision
            return decision

        best_decision = ActivityValidationDecision()
        self._verify_school_provider_search_attempted_count += 1
        validation_documents: list[SearchDocument] = []
        seen_validation_urls: set[str] = set()
        for target_name, query_limit, page_limit in (
            ("school_chapter_list", 4, 3),
            # This fallback stays constrained to official-school evidence but gives
            # the validator a second chance when the roster-specific queries miss
            # and a school Greek-life/OFSL page can still lead us to the roster.
            ("website_school", 2, 2),
        ):
            for document in self._build_validation_documents(
                job,
                target=target_name,
                query_limit=query_limit,
                page_limit=page_limit,
                require_official_school=True,
            ):
                key = _normalize_url(document.url or f"{target_name}:{document.title}:{document.query}")
                if key in seen_validation_urls:
                    continue
                seen_validation_urls.add(key)
                validation_documents.append(document)

        for document in validation_documents:
            if not document.url:
                continue
            decision = tool_school_chapter_list_validator(
                school_name=school_name,
                fraternity_name=self._fraternity_name_for_job(job),
                fraternity_slug=fraternity_slug,
                page_url=document.url,
                title=document.title or "",
                text=document.text,
                html=document.html or "",
            )
            if decision.decision == "confirmed_inactive":
                best_decision = ActivityValidationDecision(
                    chapter_activity_status="confirmed_inactive",
                    evidence_url=document.url,
                    evidence_source_type="official_school",
                    reason_code=(decision.reason_codes or ["chapter_inactive"])[0],
                    source_snippet=document.text[:400],
                    confidence=decision.confidence,
                    metadata=decision.as_dict(),
                )
                break
            if decision.decision == "confirmed_active" and best_decision.chapter_activity_status == "unknown":
                best_decision = ActivityValidationDecision(
                    chapter_activity_status="confirmed_active",
                    evidence_url=document.url,
                    evidence_source_type="official_school",
                    reason_code=(decision.reason_codes or ["chapter_active"])[0],
                    source_snippet=document.text[:400],
                    confidence=decision.confidence,
                    metadata=decision.as_dict(),
                )

        if best_decision.chapter_activity_status != "unknown":
            record = self._repository.upsert_chapter_activity(
                fraternity_slug=fraternity_slug,
                school_name=school_name,
                chapter_activity_status=best_decision.chapter_activity_status,
                confidence=best_decision.confidence,
                evidence_url=best_decision.evidence_url,
                evidence_source_type=best_decision.evidence_source_type,
                reason_code=best_decision.reason_code,
                metadata={**best_decision.metadata, "sourceSnippet": best_decision.source_snippet},
            )
            decision = self._decision_from_chapter_activity(record)
            self._chapter_activity_cache[cache_key] = decision
            return decision

        self._chapter_activity_cache[cache_key] = best_decision
        return best_decision

    def _mark_chapter_inactive(self, job: FieldJob, *, target_field: str, decision: ActivityValidationDecision) -> FieldJobResult:
        sibling_field_names = [
            field_name
            for state_key, field_name in _STATE_KEY_TO_FIELD_JOB.items()
            if state_key != target_field
        ]
        self._repository.apply_chapter_inactive_status(
            chapter_id=job.chapter_id,
            chapter_slug=job.chapter_slug,
            fraternity_slug=job.fraternity_slug,
            source_slug=job.source_slug,
            crawl_run_id=job.crawl_run_id,
            reason_code=decision.reason_code or "inactive_by_school_validation",
            evidence_url=decision.evidence_url,
            evidence_source_type=decision.evidence_source_type,
            source_snippet=decision.source_snippet,
            provider="field_job_validation",
            metadata=decision.metadata,
        )
        self._repository.complete_pending_field_jobs_for_chapter(
            chapter_id=job.chapter_id,
            reason_code=decision.reason_code or "inactive_by_school_validation",
            status="inactive_by_school_validation",
            field_states={state_key: "inactive" for state_key in _STATE_KEY_TO_FIELD_JOB if state_key != target_field},
            field_names=sibling_field_names,
        )
        return FieldJobResult(
            chapter_updates={"chapter_status": "inactive"},
            completed_payload={
                "status": "inactive_by_school_validation",
                "field": target_field,
                "reasonCode": decision.reason_code or "inactive_by_school_validation",
                "evidenceUrl": decision.evidence_url,
                "resolutionEvidence": self._resolution_evidence_for_activity_decision(
                    decision,
                    decision_stage="chapter_activity_validation",
                ),
                "decision_trace": self._build_decision_trace_summary(),
            },
            field_state_updates={target_field: "inactive"},
        )

    def _resolve_activity_gate(self, job: FieldJob, *, target_field: str) -> FieldJobResult | None:
        if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
            if job_has_canonical_active_status(job):
                self._trace(
                    "status_engine_validation",
                    status="bypassed_with_canonical_active_status",
                    school=self._school_name_for_job(job),
                )
                return None
            if job_has_existing_instagram_support(job, self._repository):
                self._trace(
                    "status_engine_validation",
                    status="bypassed_with_instagram_support",
                    school=self._school_name_for_job(job),
                )
                return None

        status_decision = self._get_or_resolve_status_decision(job)
        if status_decision is not None:
            decision = self._activity_decision_from_status_decision(
                status_decision,
                evidence_url=str(status_decision.decision_trace.get("winning_evidence_id") or "") or None,
                source_snippet=str(status_decision.decision_trace.get("final_status_basis") or "") or None,
            )
            if status_decision.final_status == ChapterStatusFinal.INACTIVE:
                self._trace("status_engine_validation", status="inactive", school=self._school_name_for_job(job))
                return self._mark_chapter_inactive(job, target_field=target_field, decision=decision)
            if status_decision.final_status == ChapterStatusFinal.ACTIVE:
                self._trace("status_engine_validation", status="active", school=self._school_name_for_job(job))
                return None
            if status_decision.final_status == ChapterStatusFinal.REVIEW:
                self._trace("status_engine_validation", status="review", school=self._school_name_for_job(job))
            else:
                self._trace("status_engine_validation", status="unknown", school=self._school_name_for_job(job))

        if job.field_name != FIELD_JOB_VERIFY_SCHOOL:
            if not self._repository.has_pending_field_job(job.chapter_id, FIELD_JOB_VERIFY_SCHOOL):
                try:
                    if job.crawl_run_id is not None and job.source_slug:
                        self._repository.create_field_jobs(
                            chapter_id=job.chapter_id,
                            crawl_run_id=job.crawl_run_id,
                            chapter_slug=job.chapter_slug,
                            source_slug=job.source_slug,
                            missing_fields=[FIELD_JOB_VERIFY_SCHOOL],
                        )
                except Exception:
                    pass
            raise RetryableJobError(
                "Status verification must complete before contact enrichment",
                backoff_seconds=self._dependency_wait_seconds,
                preserve_attempt=True,
                reason_code="status_dependency_unmet",
            )
        return None

    def _get_or_resolve_authoritative_bundle(self, job: FieldJob) -> AuthoritativeBundle:
        cached = self._authoritative_bundle_cache.get(job.chapter_id)
        if cached is not None:
            return cached

        bundle = AuthoritativeBundle()
        authoritative_documents: list[SearchDocument] = []
        school_name = self._school_name_for_job(job)
        fraternity_name = self._fraternity_name_for_job(job)

        for document in self._build_validation_documents(
            job,
            target="school_chapter_list",
            query_limit=3,
            page_limit=2,
            require_official_school=True,
        ):
            authoritative_documents.append(document)

        authoritative_documents = [
            document
            for document in authoritative_documents
            if document.url
            and tool_site_scope_classifier(
                page_url=document.url,
                title=document.title or "",
                text=document.text,
                fraternity_name=fraternity_name,
                school_name=school_name,
                chapter_name=job.chapter_name,
            ).decision in {"school_affiliation", "nationals", "chapter_site"}
        ]

        if authoritative_documents:
            bundle.authoritative_context_found = True
            bundle.evidence_url = authoritative_documents[0].url
            bundle.evidence_source_type = "official_school" if _website_trust_tier(job, authoritative_documents[0].url or "") == "tier1" else "nationals"

        website_matches: list[CandidateMatch] = []
        email_matches: list[CandidateMatch] = []
        instagram_matches: list[CandidateMatch] = []
        for document in authoritative_documents:
            website_matches.extend(self._extract_website_matches(document, job))
            email_matches.extend(self._extract_email_matches(document, job))
            instagram_matches.extend(self._extract_instagram_matches(document, job))
            relaxed_email, relaxed_instagram = self._extract_relaxed_authoritative_matches(document, job)
            if relaxed_email is not None:
                email_matches.append(relaxed_email)
            if relaxed_instagram is not None:
                instagram_matches.append(relaxed_instagram)

        bundle.website_match = _best_match(website_matches)
        bundle.email_match = _best_match(email_matches)
        bundle.instagram_match = _best_match(instagram_matches)
        activity_decision = self._get_or_resolve_chapter_activity(job)
        if (
            activity_decision.chapter_activity_status == "confirmed_active"
            and bundle.authoritative_context_found
            and bundle.website_match is None
        ):
            bundle.website_confirmed_absent = True
            bundle.reason_code = "authoritative_no_website_found"
            if activity_decision.evidence_url and not bundle.evidence_url:
                bundle.evidence_url = activity_decision.evidence_url
                bundle.evidence_source_type = activity_decision.evidence_source_type
        self._authoritative_bundle_cache[job.chapter_id] = bundle
        return bundle

    def _extract_relaxed_authoritative_matches(
        self,
        document: SearchDocument,
        job: FieldJob,
    ) -> tuple[CandidateMatch | None, CandidateMatch | None]:
        if not document.url or _website_trust_tier(job, document.url) != "tier1":
            return None, None
        scope = tool_site_scope_classifier(
            page_url=document.url,
            title=document.title or "",
            text=document.text,
            fraternity_name=self._fraternity_name_for_job(job),
            school_name=self._school_name_for_job(job),
            chapter_name=job.chapter_name,
        )
        if scope.decision != "school_affiliation":
            return None, None

        relaxed_email: CandidateMatch | None = None
        relaxed_instagram: CandidateMatch | None = None
        for link in document.links:
            if relaxed_email is None:
                email = sanitize_as_email(link)
                if email and _email_looks_relevant_to_job(email, job, document=document):
                    relaxed_email = CandidateMatch(
                        value=email,
                        confidence=0.9,
                        source_url=document.url,
                        source_snippet=document.text[:400],
                        field_name="contact_email",
                        source_provider=document.provider,
                        related_website_url=document.url,
                        query=document.query,
                    )
            if relaxed_instagram is None:
                instagram = sanitize_as_instagram(link)
                if instagram and _instagram_looks_relevant_to_job(instagram, job, document=document):
                    relaxed_instagram = CandidateMatch(
                        value=instagram,
                        confidence=0.9,
                        source_url=document.url,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        source_provider=document.provider,
                        related_website_url=document.url,
                        query=document.query,
                    )
            if relaxed_email is not None and relaxed_instagram is not None:
                break
        return relaxed_email, relaxed_instagram

    def _authoritative_bundle_result(self, job: FieldJob, *, target_field: str) -> FieldJobResult | None:
        bundle = self._get_or_resolve_authoritative_bundle(job)
        state_updates: dict[str, str] = {}
        chapter_updates: dict[str, str] = {}

        for field_key, match in (
            ("website_url", bundle.website_match),
            ("contact_email", bundle.email_match),
            ("instagram_url", bundle.instagram_match),
        ):
            if match is None:
                continue
            evidence = self._resolution_evidence_for_candidate(job, match, target_field=field_key, decision_stage="authoritative_bundle")
            if field_key == "website_url" and evidence.get("pageScope") == PAGE_SCOPE_NATIONALS_GENERIC:
                self._trace("authoritative_match_rejected", field=field_key, reason="nationals_generic_page")
                continue
            if field_key in {"contact_email", "instagram_url"} and evidence.get("contactSpecificity") == CONTACT_SPECIFICITY_NATIONAL_GENERIC:
                self._trace("authoritative_match_rejected", field=field_key, reason="nationals_generic_contact")
                continue
            chapter_updates[field_key] = match.value
            state_updates[field_key] = "found"

        if not chapter_updates and not (target_field == "website_url" and bundle.website_confirmed_absent):
            return None

        sibling_fields_to_cancel = [
            _STATE_KEY_TO_FIELD_JOB[field_key]
            for field_key, value in chapter_updates.items()
            if field_key != target_field and field_key in _STATE_KEY_TO_FIELD_JOB
        ]
        if sibling_fields_to_cancel:
            self._repository.complete_pending_field_jobs_for_chapter(
                chapter_id=job.chapter_id,
                reason_code="resolved_from_authoritative_source",
                status="resolved_from_authoritative_source",
                chapter_updates={
                    field_key: chapter_updates[field_key]
                    for field_key in chapter_updates
                    if field_key != target_field
                },
                field_states={field_key: state_updates[field_key] for field_key in chapter_updates if field_key != target_field},
                field_names=sibling_fields_to_cancel,
            )

        if target_field == "website_url" and bundle.website_confirmed_absent and "website_url" not in chapter_updates:
            state_updates["website_url"] = "confirmed_absent"
            return FieldJobResult(
                chapter_updates=chapter_updates,
                completed_payload={
                    "status": "confirmed_absent",
                    "field": "website_url",
                    "reasonCode": bundle.reason_code or "authoritative_no_website_found",
                    "evidenceUrl": bundle.evidence_url,
                    "resolutionEvidence": {
                        **self._resolution_evidence_for_authoritative_bundle(job, bundle, target_field="website_url"),
                        "reasonCode": bundle.reason_code or "authoritative_no_website_found",
                    },
                    "decision_trace": self._build_decision_trace_summary(),
                },
                field_state_updates=state_updates,
            )

        if target_field not in chapter_updates:
            return None

        return FieldJobResult(
            chapter_updates=chapter_updates,
            completed_payload={
                "status": "updated",
                "field": target_field,
                "sourceUrl": (bundle.website_match or bundle.email_match or bundle.instagram_match).source_url if (bundle.website_match or bundle.email_match or bundle.instagram_match) is not None else bundle.evidence_url,
                "resolutionEvidence": self._resolution_evidence_for_authoritative_bundle(job, bundle, target_field=target_field),
                "decision_trace": self._build_decision_trace_summary(),
            },
            field_state_updates=state_updates,
        )

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

        should_verify_website = (
            target_field == "website_url"
            and (
                match.source_provider in {"search_result", "search_page"}
                or _website_candidate_looks_low_signal(match.value)
                or _candidate_is_source_domain(match.value, job)
            )
        )
        if should_verify_website:
            verification_source = SearchDocument(
                text=match.source_snippet,
                url=match.source_url,
                title="",
                provider=match.source_provider,
                query=match.query,
            )
            verified_value, verification_document = self._website_verification_document(job, match.value, verification_source)
            match.value = verified_value
            verification = _verify_official_website_candidate(
                job,
                match.value,
                verification_document,
            )
            if verification.decision == "reject":
                self._trace(
                    "candidate_rejected",
                    target=target_field,
                    reason="official_domain_verifier_rejected",
                    verifier_reason=",".join(verification.reason_codes),
                )
                raise self._no_candidate_error(job, f"Candidate for {target_field} failed official-domain verification")
            self._trace(
                "official_domain_verified",
                target=target_field,
                decision=verification.decision,
                confidence=round(verification.confidence, 4),
            )

        resolution_evidence = self._resolution_evidence_for_candidate(job, match, target_field=target_field, decision_stage="search_candidate")
        if target_field == "website_url" and resolution_evidence.get("pageScope") == PAGE_SCOPE_NATIONALS_GENERIC:
            self._trace("candidate_rejected", target=target_field, reason="nationals_generic_website")
            raise self._no_candidate_error(job, "Generic nationals page does not count as a chapter website")
        if target_field in {"contact_email", "instagram_url"} and resolution_evidence.get("contactSpecificity") == CONTACT_SPECIFICITY_NATIONAL_GENERIC:
            self._trace("candidate_rejected", target=target_field, reason="nationals_generic_contact")
            raise self._no_candidate_error(job, f"Generic nationals {target_field} does not count as chapter contact")

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
                    "resolutionEvidence": resolution_evidence,
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
                if not current_website and match.source_provider in {"search_result", "search_page"}:
                    related_verification = _verify_official_website_candidate(
                        job,
                        sanitized_related_website,
                        SearchDocument(
                            text=match.source_snippet,
                            url=match.source_url,
                            title="",
                            provider=match.source_provider,
                            query=match.query,
                        ),
                    )
                    if related_verification.decision == "reject":
                        self._trace(
                            "related_website_rejected",
                            target=target_field,
                            reason="official_domain_verifier_rejected",
                        )
                        sanitized_related_website = None
                    else:
                        self._trace(
                            "related_website_verified",
                            target=target_field,
                            decision=related_verification.decision,
                            confidence=round(related_verification.confidence, 4),
                        )
            if sanitized_related_website and (not current_website or current_website == sanitized_related_website):
                chapter_updates["website_url"] = sanitized_related_website
                field_state_updates["website_url"] = "found" if match.confidence >= found_threshold else "low_confidence"

        completed_payload = {
            "status": "updated",
            target_field: match.value,
            "confidence": f"{match.confidence:.2f}",
            "source_url": match.source_url,
            "provider": match.source_provider,
            "resolutionEvidence": resolution_evidence,
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
        raw_website = str(job.website_url or "").strip()
        if raw_website and not raw_website.lower().startswith(("http://", "https://")):
            chapter_updates: dict[str, Any] = {"website_url": None}
            completed_payload: dict[str, str] = {
                "status": "invalid_candidate",
                "website_url": raw_website,
                "reason_code": "invalid_candidate",
            }
            field_state_updates = {"website_url": "missing"}
            if raw_website.lower().startswith("mailto:"):
                mailto_match = _MAILTO_RE.match(raw_website)
                candidate_email = (mailto_match.group(1).strip().lower() if mailto_match else "").strip()
                if candidate_email and not (job.contact_email or "").strip():
                    chapter_updates["contact_email"] = candidate_email
                    field_state_updates["contact_email"] = "found"
                    completed_payload["contact_email"] = candidate_email
            return FieldJobResult(
                chapter_updates=chapter_updates,
                completed_payload=completed_payload,
                field_state_updates=field_state_updates,
            )
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
        chapter_school_name = _canonical_school_name(job.university_name)
        candidate_school_name = _canonical_school_name(job.payload.get("candidateSchoolName"))
        chapter_school = _slugify(chapter_school_name)
        candidate_school = _slugify(candidate_school_name)
        if chapter_school and candidate_school and chapter_school == candidate_school:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={"status": "verified", "university_name": chapter_school_name or job.university_name or ""},
                field_state_updates={"university_name": "found"},
            )
        if chapter_school and candidate_school and chapter_school != candidate_school:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={
                    "status": "mismatch_reviewed",
                    "stored_university_name": chapter_school_name or job.university_name or "",
                    "candidate_school_name": candidate_school_name or str(job.payload.get("candidateSchoolName") or ""),
                },
                review_item=ReviewItemCandidate(
                    item_type="school_match_mismatch",
                    reason="Candidate school name does not match the stored university name",
                    source_slug=job.payload.get("sourceSlug") if isinstance(job.payload.get("sourceSlug"), str) else None,
                    chapter_slug=job.chapter_slug,
                    payload={
                        "storedUniversityName": chapter_school_name or job.university_name,
                        "candidateSchoolName": candidate_school_name or job.payload.get("candidateSchoolName"),
                    },
                ),
            )
        if chapter_school and not candidate_school:
            status_decision = self._get_or_resolve_status_decision(job)
            if status_decision is not None:
                decision = self._activity_decision_from_status_decision(
                    status_decision,
                    evidence_url=str(status_decision.decision_trace.get("winning_evidence_id") or "") or None,
                    source_snippet=str(status_decision.decision_trace.get("final_status_basis") or "") or None,
                )
                if status_decision.final_status == ChapterStatusFinal.INACTIVE:
                    self._trace("status_engine_validation", status="inactive", school=self._school_name_for_job(job))
                    return self._mark_chapter_inactive(job, target_field="university_name", decision=decision)
                if status_decision.final_status == ChapterStatusFinal.ACTIVE:
                    self._trace("status_engine_validation", status="active", school=self._school_name_for_job(job))
                    return FieldJobResult(
                        chapter_updates={},
                        completed_payload={
                            "status": "verified",
                            "university_name": job.university_name or "",
                            "reasonCode": status_decision.reason_code,
                            "resolutionEvidence": self._resolution_evidence_for_activity_decision(
                                decision,
                                decision_stage="chapter_status_engine",
                            ),
                            "decision_trace": self._build_decision_trace_summary(),
                        },
                        field_state_updates={"university_name": "found"},
                    )
                if status_decision.final_status == ChapterStatusFinal.REVIEW:
                    return FieldJobResult(
                        chapter_updates={},
                        completed_payload={
                            "status": "review_required",
                            "reasonCode": status_decision.reason_code,
                            "resolutionEvidence": self._resolution_evidence_for_activity_decision(
                                decision,
                                decision_stage="chapter_status_engine",
                            ),
                            "decision_trace": self._build_decision_trace_summary(),
                        },
                        review_item=ReviewItemCandidate(
                            item_type="school_match_mismatch",
                            reason="Status engine could not produce a safe active/inactive school verification decision",
                            source_slug=job.payload.get("sourceSlug") if isinstance(job.payload.get("sourceSlug"), str) else None,
                            chapter_slug=job.chapter_slug,
                            payload={
                                "statusDecisionId": status_decision.id,
                                "reasonCode": status_decision.reason_code,
                                "conflictFlags": list(status_decision.conflict_flags),
                            },
                        ),
                    )

            school_policy = self._get_or_resolve_school_policy(job)
            if school_policy.school_policy_status == "allowed":
                self._trace("campus_policy_validation", status="allowed", school=self._school_name_for_job(job))
                return FieldJobResult(
                    chapter_updates={},
                    completed_payload={
                        "status": "verified",
                        "university_name": job.university_name or "",
                        "reasonCode": school_policy.reason_code or "school_policy_allowed",
                        "resolutionEvidence": self._resolution_evidence_for_activity_decision(
                            school_policy,
                            decision_stage="campus_policy_validation",
                        ),
                        "decision_trace": self._build_decision_trace_summary(),
                    },
                    field_state_updates={"university_name": "found"},
                )
        raise self._unresolved_validation_retry(job, "Insufficient school data to verify school match")

    def _unresolved_validation_retry(self, job: FieldJob, message: str) -> RetryableJobError:
        if self._search_skipped_due_to_degraded_mode:
            return RetryableJobError(
                f"{message}; search preflight degraded",
                backoff_seconds=max(
                    self._transient_long_cooldown_seconds,
                    self._dependency_wait_seconds,
                    self._base_backoff_seconds,
                ),
                preserve_attempt=True,
                reason_code="provider_degraded",
            )
        all_queries_failed = self._search_queries_attempted > 0 and self._search_queries_failed >= self._search_queries_attempted
        if self._provider_search_hard_blocked() or (self._search_errors_encountered and all_queries_failed):
            return RetryableJobError(
                f"{message}; official-school search provider or network unavailable",
                backoff_seconds=max(self._dependency_wait_seconds, self._base_backoff_seconds),
                preserve_attempt=True,
                reason_code="transient_network",
            )
        if self._search_errors_encountered:
            return RetryableJobError(
                f"{message}; official-school search low signal",
                backoff_seconds=max(self._dependency_wait_seconds, self._base_backoff_seconds),
                preserve_attempt=True,
                reason_code="provider_low_signal",
            )
        return RetryableJobError(message, reason_code="dependency_wait")

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

    def _build_instagram_identity(self, job: FieldJob):
        fraternity_display = _display_name(job.fraternity_slug)
        return build_chapter_instagram_identity(
            fraternity_name=fraternity_display,
            fraternity_slug=job.fraternity_slug,
            school_name=self._school_name_for_job(job),
            chapter_name=job.chapter_name,
            school_aliases=_school_aliases(
                self._school_name_for_job(job),
                enable_school_initials=self._enable_school_initials,
                min_school_initial_length=self._min_school_initial_length,
            ),
            fraternity_aliases=_fraternity_query_aliases(fraternity_display, job.fraternity_slug),
        )

    def _instagram_source_provider(self, source_type: InstagramSourceType) -> str:
        if source_type in {
            InstagramSourceType.NATIONALS_CHAPTER_ENTRY,
            InstagramSourceType.NATIONALS_CHAPTER_PAGE,
            InstagramSourceType.NATIONALS_DIRECTORY_ROW,
        }:
            return "nationals_directory"
        if source_type in {
            InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
            InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA,
            InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK,
        }:
            return "chapter_website"
        if source_type in {
            InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
            InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
        }:
            return "search_page"
        if source_type in {
            InstagramSourceType.SEARCH_RESULT_PROFILE,
            InstagramSourceType.GENERATED_HANDLE_SEARCH,
        }:
            return "search_result"
        if source_type in {
            InstagramSourceType.PROVENANCE_SUPPORTING_PAGE,
            InstagramSourceType.AUTHORITATIVE_BUNDLE,
        }:
            return "provenance"
        return "instagram_candidate_bank"

    def _resolution_evidence_for_instagram_candidate(
        self,
        candidate,
        *,
        reason_code: str,
        decision_stage: str,
        extra_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        page_scope = candidate.page_scope
        if not page_scope:
            if candidate.source_type in {
                InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
                InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
            }:
                page_scope = PAGE_SCOPE_SCHOOL_AFFILIATION
            elif candidate.source_type in {
                InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
                InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA,
                InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK,
            }:
                page_scope = PAGE_SCOPE_CHAPTER_SITE
            elif candidate.source_type in {
                InstagramSourceType.NATIONALS_CHAPTER_ENTRY,
                InstagramSourceType.NATIONALS_CHAPTER_PAGE,
                InstagramSourceType.NATIONALS_DIRECTORY_ROW,
            }:
                page_scope = PAGE_SCOPE_NATIONALS_CHAPTER
            else:
                page_scope = PAGE_SCOPE_UNRELATED
        metadata = {
            "query": candidate.metadata.get("query"),
            "relatedWebsiteUrl": candidate.metadata.get("relatedWebsiteUrl"),
            **(extra_metadata or {}),
        }
        return {
            "decisionStage": decision_stage,
            "evidenceUrl": candidate.evidence_url or candidate.source_url or candidate.profile_url,
            "sourceType": str(candidate.source_type),
            "pageScope": page_scope,
            "contactSpecificity": candidate.contact_specificity or _contact_specificity_for_page_scope(page_scope),
            "confidence": round(float(candidate.confidence or 0.0), 4),
            "reasonCode": reason_code,
            "metadata": {key: value for key, value in metadata.items() if value is not None},
        }

    def _candidate_match_from_instagram_candidate(self, candidate) -> CandidateMatch:
        source_url = candidate.evidence_url or candidate.source_url or candidate.profile_url
        source_snippet = (
            candidate.source_snippet
            or candidate.local_container_text
            or candidate.surrounding_text
            or candidate.handle
        )
        return CandidateMatch(
            value=candidate.profile_url,
            confidence=float(candidate.confidence or 0.0),
            source_url=source_url,
            source_snippet=source_snippet[:400],
            field_name="instagram_url",
            source_provider=self._instagram_source_provider(candidate.source_type),
            related_website_url=str(candidate.metadata.get("relatedWebsiteUrl") or "") or None,
            query=str(candidate.metadata.get("query") or "") or None,
        )

    def _fetch_instagram_bank_candidates(self, job: FieldJob):
        fetcher = getattr(self._repository, "fetch_instagram_candidates_for_chapters", None)
        if fetcher is None:
            return []
        rows = fetcher([job.chapter_id]) or []
        bank = InstagramCandidateBank()
        for row in rows:
            candidate = candidate_from_chapter_evidence(row)
            if candidate is None:
                continue
            candidate.metadata["fraternitySlug"] = job.fraternity_slug
            bank.add_candidate(job.chapter_id, candidate)
        bank.dedupe_by_handle_and_source()
        identity = self._build_instagram_identity(job)
        return [score_instagram_candidate(candidate, identity) for candidate in bank.get_candidates_for_chapter(job.chapter_id)]

    def _find_instagram_candidate_from_candidate_bank(self, job: FieldJob) -> CandidateMatch | None:
        candidates = self._fetch_instagram_bank_candidates(job)
        accepted = [
            candidate
            for candidate in candidates
            if not candidate.reject_reasons and candidate.confidence >= instagram_write_threshold(candidate)
        ]
        accepted.sort(key=lambda item: item.confidence, reverse=True)
        return self._candidate_match_from_instagram_candidate(accepted[0]) if accepted else None

    def _instagram_audit_result(self, job: FieldJob, existing_instagram: str) -> FieldJobResult | None:
        candidates = self._fetch_instagram_bank_candidates(job)
        should_audit = self._validate_existing_instagram or (
            not self._existing_instagram_is_confident(job, existing_instagram) and bool(candidates)
        )
        if not should_audit:
            return None
        decision = audit_existing_instagram_candidate(
            chapter_id=job.chapter_id,
            existing_url=existing_instagram,
            identity=self._build_instagram_identity(job),
            candidates=candidates,
        )
        candidate = decision.accepted_candidate
        if decision.outcome == "existing_value_confirmed" and candidate is not None and decision.selected_url:
            normalized_url = sanitize_as_instagram(decision.selected_url) or decision.selected_url
            resolution_evidence = self._resolution_evidence_for_instagram_candidate(
                candidate,
                reason_code=decision.reason_code,
                decision_stage="existing_instagram_audit",
            )
            return FieldJobResult(
                chapter_updates={"instagram_url": normalized_url},
                completed_payload={
                    "status": "verified",
                    "instagram_url": normalized_url,
                    "confidence": f"{decision.confidence:.2f}",
                    "reasonCode": decision.reason_code,
                    "source_url": candidate.evidence_url or candidate.source_url or normalized_url,
                    "resolutionEvidence": resolution_evidence,
                    "decision_trace": self._build_decision_trace_summary(),
                },
                field_state_updates={"instagram_url": "found"},
                provenance_records=[
                    ProvenanceRecord(
                        source_slug=job.source_slug or "instagram_audit",
                        source_url=candidate.evidence_url or candidate.source_url or normalized_url,
                        field_name="instagram_url",
                        field_value=normalized_url,
                        source_snippet=(candidate.source_snippet or candidate.local_container_text or candidate.handle)[:400],
                        confidence=decision.confidence,
                    )
                ],
            )
        if decision.outcome == "existing_value_replaced" and candidate is not None and decision.selected_url:
            normalized_url = sanitize_as_instagram(decision.selected_url) or decision.selected_url
            resolution_evidence = self._resolution_evidence_for_instagram_candidate(
                candidate,
                reason_code=decision.reason_code,
                decision_stage="existing_instagram_audit",
                extra_metadata={"allowReplaceExisting": True, "previousUrl": decision.previous_url},
            )
            return FieldJobResult(
                chapter_updates={"instagram_url": normalized_url},
                completed_payload={
                    "status": "updated",
                    "instagram_url": normalized_url,
                    "confidence": f"{decision.confidence:.2f}",
                    "reasonCode": decision.reason_code,
                    "source_url": candidate.evidence_url or candidate.source_url or normalized_url,
                    "allowReplaceExisting": True,
                    "resolutionEvidence": resolution_evidence,
                    "decision_trace": self._build_decision_trace_summary(),
                },
                field_state_updates={"instagram_url": "found"},
                provenance_records=[
                    ProvenanceRecord(
                        source_slug=job.source_slug or "instagram_audit",
                        source_url=candidate.evidence_url or candidate.source_url or normalized_url,
                        field_name="instagram_url",
                        field_value=normalized_url,
                        source_snippet=(candidate.source_snippet or candidate.local_container_text or candidate.handle)[:400],
                        confidence=decision.confidence,
                    )
                ],
            )
        if decision.outcome == "review_required":
            resolution_evidence = None
            if candidate is not None:
                resolution_evidence = self._resolution_evidence_for_instagram_candidate(
                    candidate,
                    reason_code=decision.reason_code,
                    decision_stage="existing_instagram_audit",
                    extra_metadata={"previousUrl": decision.previous_url},
                )
            completed_payload = {
                "status": "review_required",
                "reasonCode": decision.reason_code,
                "previousUrl": decision.previous_url,
                "decision_trace": self._build_decision_trace_summary(),
            }
            if resolution_evidence is not None:
                completed_payload["resolutionEvidence"] = resolution_evidence
            return FieldJobResult(
                chapter_updates={},
                completed_payload=completed_payload,
                field_state_updates={"instagram_url": "low_confidence"},
                review_item=ReviewItemCandidate(
                    item_type="instagram_candidate",
                    reason=decision.reason_code,
                    source_slug=job.source_slug,
                    chapter_slug=job.chapter_slug,
                    payload={
                        "fieldName": "instagram_url",
                        "candidateValue": decision.selected_url,
                        "previousUrl": decision.previous_url,
                        "decisionTrace": self._build_decision_trace_summary(),
                    },
                ),
            )
        return None

    def _find_instagram_candidate(self, job: FieldJob) -> CandidateMatch | None:
        matches: list[CandidateMatch] = []
        current_website = _current_website_url(job)
        probe_attempted = False

        if current_website and _website_trust_tier(job, current_website) == "tier1":
            self._trace("instagram_strategy", stage="trusted_chapter_website")
            website_document = self._fetch_search_document(current_website, provider="chapter_website")
            if website_document is not None:
                website_matches = self._extract_instagram_matches(website_document, job)
                matches.extend(website_matches)
                best_website = _best_match(website_matches)
                if best_website is not None and best_website.confidence >= self._found_threshold(job, "instagram_url", best_website):
                    return best_website

        self._trace("instagram_strategy", stage="provenance")
        provenance_match = self._find_instagram_candidate_from_provenance(job)
        if provenance_match is not None:
            matches.append(provenance_match)
            if provenance_match.confidence >= self._found_threshold(job, "instagram_url", provenance_match):
                return provenance_match

        self._trace("instagram_strategy", stage="nationals")
        nationals_matches = self._find_target_candidates_from_nationals(job, target="instagram")
        if nationals_matches:
            matches.extend(nationals_matches)
            best_nationals = _best_match(matches)
            if best_nationals is not None and best_nationals.confidence >= self._found_threshold(job, "instagram_url", best_nationals):
                return best_nationals

        if job.source_base_url:
            self._trace("instagram_strategy", stage="source_page")
            source_document = self._fetch_search_document(job.source_base_url, provider="source_page")
            if source_document is not None:
                source_matches = self._extract_instagram_matches(source_document, job)
                matches.extend(source_matches)
                best_source = _best_match(matches)
                if best_source is not None and best_source.confidence >= self._found_threshold(job, "instagram_url", best_source):
                    return best_source

        self._trace("instagram_strategy", stage="candidate_bank")
        candidate_bank_match = self._find_instagram_candidate_from_candidate_bank(job)
        if candidate_bank_match is not None:
            matches.append(candidate_bank_match)
            if candidate_bank_match.confidence >= self._found_threshold(job, "instagram_url", candidate_bank_match):
                return candidate_bank_match

        if self._instagram_direct_probe_enabled:
            probe_attempted = True
            self._trace("instagram_strategy", stage="direct_handle_probe")
            probe_matches = self._probe_instagram_handle_candidates(job)
            if probe_matches:
                matches.extend(probe_matches)
                best_probe = _best_match(matches)
                if best_probe is not None and best_probe.confidence >= self._found_threshold(job, "instagram_url", best_probe):
                    return best_probe

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
        if self._instagram_direct_probe_enabled and not probe_attempted and not (
            best_external is not None and best_external.confidence >= 0.88
        ):
            self._trace("instagram_strategy", stage="direct_handle_probe")
            probe_matches = self._probe_instagram_handle_candidates(job)
            if probe_matches:
                matches.extend(probe_matches)
                best_probe = _best_match(matches)
                if best_probe is not None and best_probe.confidence >= self._found_threshold(job, "instagram_url", best_probe):
                    return best_probe
            best_external = _best_match(matches)

        if best_external is not None and best_external.confidence >= 0.88:
            return best_external

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
        if self._search_degraded_mode:
            self._search_skipped_due_to_degraded_mode = True
            self._trace("external_search_skipped", target="trusted_school_email", reason="preflight_degraded")
            return []
        matches: list[CandidateMatch] = []
        seen_urls: set[str] = set()
        fetched_pages = 0
        query_limit = min(2, max(1, self._email_max_queries))
        for query in self._build_search_queries(job, target="website_school")[:query_limit]:
            query_results = self._run_search(query)
            if self._maybe_abort_search_sequence(job, target="trusted_school_email", query_results=query_results):
                break
            for result in query_results:
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
            field_state_updates={"instagram_url": "inactive"},
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

        if self._greedy_collect_mode == _GREEDY_COLLECT_BFS and not self._search_degraded_mode:
            fraternity = _display_name(job.fraternity_slug)
            for query in [
                f'site:{source_host} "{fraternity}" "chapter directory"',
                f'site:{source_host} "{fraternity}" chapters',
            ]:
                query_results = self._run_search(query)
                if self._maybe_abort_search_sequence(job, target="nationals_directory_seed", query_results=query_results):
                    break
                for result in query_results[:3]:
                    result_host = (urlparse(result.url).netloc or "").lower()
                    if result_host == source_host or result_host.endswith(f".{source_host}"):
                        seed_urls.append(result.url)
        elif self._greedy_collect_mode == _GREEDY_COLLECT_BFS:
            self._search_skipped_due_to_degraded_mode = True
            self._trace("external_search_skipped", target="nationals_directory_seed", reason="preflight_degraded")

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

        if self._search_degraded_mode:
            self._search_skipped_due_to_degraded_mode = True
            self._trace("external_search_skipped", target=target, reason="preflight_degraded")
            return documents
        if self._provider_search_hard_blocked():
            self._trace("external_search_skipped", target=target, reason="provider_unavailable")
            return documents

        seen_urls: set[str] = set()
        fetched_pages = 0
        queries = self._build_search_queries(job, target)
        for query in queries:
            query_results = self._run_search(query)
            if self._maybe_abort_search_sequence(job, target=target, query_results=query_results):
                break

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

    def _maybe_abort_search_sequence(self, job: FieldJob, *, target: str, query_results: list[SearchResult]) -> bool:
        if not self._should_abort_search_fanout(query_results):
            return False
        self._search_fanout_aborted = True
        reason = self._last_search_failure_kind or "provider_unavailable"
        self._trace(
            "search_query_fanout_aborted",
            attempted_queries=self._search_queries_attempted,
            reason=reason,
            target=target,
        )
        log_event(
            self._logger,
            "search_query_fanout_aborted",
            chapter_slug=job.chapter_slug,
            field_name=job.field_name,
            attempted_queries=self._search_queries_attempted,
            reason=reason,
            target=target,
        )
        return True

    def _provider_search_hard_blocked(self) -> bool:
        if not self._search_errors_encountered:
            return False
        if self._search_queries_succeeded > 0:
            return False
        if self._last_search_failure_kind not in {"unavailable", "request_exception"}:
            return False
        if not self._search_fanout_aborted:
            return False
        return self._search_queries_failed >= max(1, self._search_queries_attempted)

    def _build_search_queries(self, job: FieldJob, target: str) -> list[str]:
        fraternity = _display_name(job.fraternity_slug)
        quoted_fraternity = f'"{fraternity}"' if fraternity else ""
        chapter = job.chapter_name or _display_name(job.chapter_slug)
        university = self._school_name_for_job(job)
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
        elif target == "school_chapter_list":
            for domain in campus_domains:
                query_parts.extend(
                    [
                        [f'"{university}"' if university else "", "chapters at", f"site:{domain}"],
                        [f'"{university}"' if university else "", "fraternities", f"site:{domain}"],
                        [f'"{university}"' if university else "", "greek life", f"site:{domain}"],
                        [f'"{university}"' if university else "", "fraternity and sorority life", f"site:{domain}"],
                        [f'"{university}"' if university else "", "fraternity chapters", f"site:{domain}"],
                        [f'"{university}"' if university else "", "recognized chapters", f"site:{domain}"],
                        [f'"{university}"' if university else "", "chapter scorecards", f"site:{domain}"],
                        [f'"{university}"' if university else "", "community scorecard", f"site:{domain}"],
                        [f'"{university}"' if university else "", "councils and chapters", f"site:{domain}"],
                        [f'"{university}"' if university else "", "student organizations", "fraternities", f"site:{domain}"],
                    ]
                )
            query_parts.extend(
                [
                    [f'"{university}"' if university else "", "chapters at", "site:.edu"],
                    [f'"{university}"' if university else "", "fraternities", "site:.edu"],
                    [f'"{university}"' if university else "", "greek life", "site:.edu"],
                    [f'"{university}"' if university else "", "fraternity and sorority life", "site:.edu"],
                    [f'"{university}"' if university else "", "fraternity chapters", "site:.edu"],
                    [f'"{university}"' if university else "", "recognized chapters", "site:.edu"],
                    [f'"{university}"' if university else "", "chapter scorecards", "site:.edu"],
                    [f'"{university}"' if university else "", "community scorecard", "site:.edu"],
                    [f'"{university}"' if university else "", "councils and chapters", "site:.edu"],
                    [f'"{university}"' if university else "", "student organizations", "fraternities", "site:.edu"],
                ]
            )
        elif target == "campus_policy":
            for domain in campus_domains:
                query_parts.extend(
                    [
                        [f'"{university}"' if university else "", "fraternities banned", f"site:{domain}"],
                        [f'"{university}"' if university else "", "greek life", f"site:{domain}"],
                        [f'"{university}"' if university else "", "fraternity sorority life", f"site:{domain}"],
                    ]
                )
            query_parts.extend(
                [
                    [f'"{university}"' if university else "", "fraternities banned", "site:.edu"],
                    [f'"{university}"' if university else "", "greek life", "site:.edu"],
                    [f'"{university}"' if university else "", "fraternity sorority life", "site:.edu"],
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
            identity_queries = build_instagram_search_queries(
                identity=self._build_instagram_identity(job),
                school_domains=campus_domains,
                chapter_website_url=_current_website_url(job),
                max_queries=max(self._instagram_max_queries, 5),
            )
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
        if target == "instagram":
            queries = [*queries, *identity_queries]
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
        if query not in self._chapter_search_queries:
            self._chapter_search_queries.append(query)
        self._last_query_provider_attempts = []
        self._last_search_failure_kind = None
        try:
            results = self._search_client.search(query)
            provider_attempts = self._consume_provider_attempts()
            self._persist_provider_attempts(query=query, attempts=provider_attempts)
            self._last_query_provider_attempts = provider_attempts
            self._last_provider_attempts.extend(provider_attempts)
        except SearchUnavailableError as exc:
            self._search_errors_encountered = True
            self._search_queries_failed += 1
            provider_attempts = self._consume_provider_attempts()
            self._persist_provider_attempts(query=query, attempts=provider_attempts)
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
            self._persist_provider_attempts(query=query, attempts=provider_attempts)
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
                url=cached_document.url or url,
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

        final_url = str(getattr(response, "url", "") or url)

        soup = _parse_document_markup(html)
        links = [href.strip() for href in (node.get("href") for node in soup.select("a[href]")) if href and href.strip()]
        text = " ".join(soup.stripped_strings)
        title = soup.title.get_text(" ", strip=True) if soup.title else None
        self._search_document_cache[cache_key] = SearchDocument(
            text=text,
            links=list(links),
            url=final_url,
            title=title,
            provider="cached",
            html=html,
        )
        return SearchDocument(text=text, links=links, url=final_url, title=title, provider=provider, query=query, html=html)

    def _website_verification_document(self, job: FieldJob, candidate_url: str, source_document: SearchDocument) -> tuple[str, SearchDocument]:
        verification_document = SearchDocument(
            text=source_document.text,
            links=list(source_document.links),
            url=source_document.url,
            title=source_document.title,
            provider=source_document.provider,
            query=source_document.query,
            html=source_document.html,
        )
        candidate_tier = _website_trust_tier(job, candidate_url)
        if not _looks_like_document_asset_url(candidate_url) and candidate_tier != "blocked":
            fetched_candidate = self._fetch_search_document(candidate_url, provider="chapter_website", query=source_document.query)
            if fetched_candidate is not None:
                verification_document = fetched_candidate
                verified_url = sanitize_as_website(fetched_candidate.url or candidate_url, base_url=candidate_url)
                if verified_url:
                    candidate_url = verified_url
        return candidate_url, verification_document

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

    @property
    def require_confident_website_for_email(self) -> bool:
        return self._require_confident_website_for_email

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
        if match.source_provider == "search_page":
            return {
                "website_url": 0.96,
                "contact_email": 0.92,
                "instagram_url": 0.88,
            }.get(target_field, 0.90)
        if match.source_provider == "search_result":
            return {
                "website_url": 0.96,
                "contact_email": 0.92,
                "instagram_url": 0.90,
            }.get(target_field, 0.90)
        return 0.85

    def _no_candidate_error(self, job: FieldJob, message: str) -> RetryableJobError:
        if self._search_skipped_due_to_degraded_mode:
            return RetryableJobError(
                f"{message}; search preflight degraded",
                backoff_seconds=max(
                    self._transient_long_cooldown_seconds,
                    self._dependency_wait_seconds,
                    self._base_backoff_seconds,
                ),
                preserve_attempt=True,
                reason_code="provider_degraded",
            )
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

    def _persist_provider_attempts(self, *, query: str, attempts: list[dict[str, object]]) -> None:
        if not attempts:
            return
        insert_many = getattr(self._repository, "insert_search_provider_attempts", None)
        if not callable(insert_many):
            return
        try:
            insert_many(
                [
                    {
                        "context_type": "field_job",
                        "context_id": self._field_name,
                        "request_id": None,
                        "source_slug": self._source_slug,
                        "field_job_id": None,
                        "provider": attempt.get("provider"),
                        "provider_endpoint": attempt.get("provider_endpoint"),
                        "query": query,
                        "status": attempt.get("status"),
                        "failure_type": attempt.get("failure_type"),
                        "http_status": attempt.get("http_status"),
                        "latency_ms": attempt.get("latency_ms"),
                        "result_count": attempt.get("result_count"),
                        "fallback_taken": bool(attempt.get("fallback_taken", False)),
                        "metadata": {
                            "providerContext": "field_job",
                            "fieldName": self._field_name,
                            "circuitOpen": bool(attempt.get("circuit_open", False)),
                        },
                    }
                    for attempt in attempts
                ]
            )
        except Exception:  # pragma: no cover - additive telemetry should not break field-job execution
            return

    def _build_requeue_payload_patch(self, job: FieldJob, exc: "RetryableJobError", backoff_seconds: int) -> dict[str, object]:
        patch: dict[str, object] = {}
        if exc.reason_code in {"transient_network", "provider_low_signal", "provider_degraded"}:
            queue_state = "blocked_provider"
        elif exc.reason_code in {"dependency_wait", "website_required", "status_dependency_unmet"}:
            queue_state = "blocked_dependency"
        else:
            queue_state = "actionable"
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
        if exc.reason_code == "provider_degraded":
            patch["provider_degraded_at"] = "preflight"
        return patch

    def _should_abort_search_fanout(self, query_results: list[SearchResult]) -> bool:
        if query_results:
            return False
        if self._search_queries_succeeded > 0:
            return False
        if self._last_search_failure_kind not in {"unavailable", "request_exception"}:
            return False
        provider_attempts = self._last_query_provider_attempts or []
        if not provider_attempts:
            return False
        all_provider_attempts_unavailable = all(
            str(attempt.get("status") or "").strip().lower() in {"unavailable", "request_error"}
            for attempt in provider_attempts
        )
        if not all_provider_attempts_unavailable:
            return False
        if self._search_queries_attempted >= 3 and self._search_queries_failed >= self._search_queries_attempted:
            return True
        if self._search_queries_attempted >= 2 and all(bool(attempt.get("circuit_open")) for attempt in provider_attempts):
            return True
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
        if generic_office_email and not identity_email and source_tier == "tier1":
            self._record_candidate_rejection("email", "generic_office_email")
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
        combined = _instagram_candidate_text(document, instagram_url)
        handle_score = _instagram_handle_match_score(instagram_url, job)
        overlap_score = _instagram_context_overlap_score(job, instagram_url, document)
        school_match = _school_matches(job, combined)
        fraternity_match = _fraternity_matches(job, combined)
        chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
        chapter_designation = _chapter_designation_signal(job, combined)
        source_tier = _website_trust_tier(job, document.url or "")
        handle_has_fraternity = _instagram_handle_has_fraternity_token(instagram_url, job)
        local_identity = _instagram_handle_has_local_identity(instagram_url, job)
        school_brand_handle = _instagram_handle_looks_like_school_brand(instagram_url, job)
        effective_local_identity = local_identity and not school_brand_handle
        if document.provider == "source_page":
            if _instagram_handle_looks_national_generic(instagram_url, job):
                self._record_candidate_rejection("instagram", "national_generic_handle")
                return False
            if _instagram_looks_institutional_or_directory_account(instagram_url, document) or (
                school_brand_handle and not (effective_local_identity or chapter_match or chapter_designation > 0)
            ):
                self._record_candidate_rejection("instagram", "institutional_account")
                return False
            if not (effective_local_identity or chapter_match or chapter_designation > 0):
                self._record_candidate_rejection("instagram", "missing_local_identity")
                return False
            if not (school_match or chapter_match or effective_local_identity):
                self._record_candidate_rejection("instagram", "missing_school_anchor")
                return False
            if not (fraternity_match or chapter_match or effective_local_identity or chapter_designation > 0):
                self._record_candidate_rejection("instagram", "missing_fraternity_anchor")
                return False
            if not effective_local_identity and not chapter_match and chapter_designation <= 0 and handle_score < 2:
                self._record_candidate_rejection("instagram", "missing_local_identity")
                return False
            return True
        if document.provider not in {"search_result", "search_page"}:
            if source_tier == "tier1":
                if _instagram_looks_institutional_or_directory_account(instagram_url, document) or (
                    school_brand_handle and not (effective_local_identity or chapter_match or chapter_designation > 0)
                ):
                    self._record_candidate_rejection("instagram", "institutional_account")
                    return False
                return (school_match or chapter_match or effective_local_identity) and (
                    fraternity_match or chapter_match or effective_local_identity or chapter_designation > 0
                )
            return True
        if source_tier == "tier1":
            if _instagram_looks_institutional_or_directory_account(instagram_url, document) or (
                school_brand_handle and not (effective_local_identity or chapter_match or chapter_designation > 0)
            ):
                self._record_candidate_rejection("instagram", "institutional_account")
                return False
            if not (handle_has_fraternity or effective_local_identity or chapter_match or chapter_designation > 0):
                self._record_candidate_rejection("instagram", "missing_local_identity")
                return False
            if not school_match and not chapter_match and not effective_local_identity:
                self._record_candidate_rejection("instagram", "missing_school_anchor")
                return False
            if not fraternity_match and not chapter_match and not effective_local_identity and chapter_designation <= 0 and handle_score < 2:
                self._record_candidate_rejection("instagram", "missing_fraternity_anchor")
                return False
            return (school_match or chapter_match or effective_local_identity) and (
                fraternity_match or chapter_match or effective_local_identity or chapter_designation > 0
            )
        if _instagram_has_conflicting_org_signal(job, combined) and handle_score < 5:
            self._record_candidate_rejection("instagram", "conflicting_org_signal")
            return False
        if _chapter_designation_signal(job, combined) < 0:
            self._record_candidate_rejection("instagram", "chapter_designation_mismatch")
            return False
        if not handle_has_fraternity and chapter_designation <= 0 and not effective_local_identity and not chapter_match:
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
            official_verification = None
            if document.provider in {"search_result", "search_page"}:
                url, verification_document = self._website_verification_document(job, url, document)
                candidate_host = (urlparse(url).netloc or "").lower()
                official_verification = _verify_official_website_candidate(job, url, verification_document)
            else:
                verification_document = document
            if (
                document.provider == "search_page"
                and _website_trust_tier(job, document.url or "") == "tier1"
                and candidate_host
                and document_host
                and candidate_host != document_host
                and candidate_host.endswith(".edu")
                and not any(candidate_host == domain or candidate_host.endswith(f".{domain}") for domain in _campus_domains(job))
            ):
                self._record_candidate_rejection("website", "cross_school_edu_link")
                continue
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
                if not _ambiguous_school_tier1_candidate_allowed(job, url, verification_document):
                    self._record_candidate_rejection("website", "ambiguous_school_tier1_generic")
                    continue
                if (
                    not _tier1_website_candidate_has_specificity(job, url, verification_document)
                    and not (
                        official_verification is not None
                        and official_verification.decision in {"official_affiliation_page", "official_chapter_domain"}
                    )
                ):
                    self._record_candidate_rejection("website", "low_specificity_tier1")
                    continue
                if official_verification.decision == "reject":
                    self._record_candidate_rejection(
                        "website",
                        "official_domain_rejected_" + (official_verification.reason_codes[0] if official_verification.reason_codes else "unknown"),
                    )
                    continue
            confidence = self._score_website_candidate(url, document, job)
            if official_verification is not None and official_verification.decision == "official_chapter_domain":
                confidence = min(0.97, confidence + 0.08)
            elif official_verification is not None and official_verification.decision == "official_affiliation_page":
                confidence = min(0.96, confidence + 0.05)
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
    while tokens and tokens[-1] in {"chapter", "colony"}:
        tokens = tokens[:-1]
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
    greek_tokens = [token for token in _normalized_match_text(display).split() if token]
    if len(greek_tokens) >= 2:
        aliases.append(" ".join(greek_tokens[:2]))
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
    compact_school = _compact_text(normalized)
    if len(compact_school) >= 4:
        aliases.append(compact_school)
        if len(compact_school) <= 8:
            aliases.append(f"{compact_school}u")
    school_initials = _school_initials(normalized)
    if enable_school_initials and len(school_initials) >= 2:
        aliases.append(school_initials.lower())
    for token in _significant_tokens(normalized):
        compact = _compact_text(token)
        if len(compact) >= 3:
            aliases.append(compact)
            if len(compact) <= 8:
                aliases.append(f"{compact}u")
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _instagram_probe_website_aliases(job: FieldJob) -> list[str]:
    website_url = _current_website_url(job)
    if not website_url:
        return []
    hostname = (urlparse(website_url).hostname or "").lower()
    if not hostname:
        return []
    parts = [part for part in hostname.split(".") if part and part not in {"www", "com", "org", "edu", "net", "ca"}]
    aliases: list[str] = []
    if parts:
        aliases.append(_compact_text(parts[0]))
    if len(parts) >= 2 and len(parts[0]) <= 3:
        aliases.append(_compact_text(parts[0] + parts[1]))
    return list(dict.fromkeys(alias for alias in aliases if 2 <= len(alias) <= 15))


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
    fraternity_tokens = [token for token in _normalized_match_text(fraternity_display).split() if token]
    if len(fraternity_tokens) >= 2:
        fraternity_aliases.append(" ".join(fraternity_tokens[:2]))
    fraternity_compacts = [
        _compact_text(alias)
        for alias in fraternity_aliases
        if len(_compact_text(alias)) >= 2
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
    school_aliases.extend(_instagram_probe_website_aliases(job))
    school_aliases = list(dict.fromkeys(alias for alias in school_aliases if alias))

    handles: list[str] = []
    for school_alias in school_aliases[:8]:
        compact_school = _compact_text(school_alias)
        if not compact_school:
            continue
        for fraternity_compact in fraternity_compacts[:8]:
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
    final_limit = max(12, max_candidates * 3)
    return list(dict.fromkeys(handle for handle in handles if 4 <= len(handle) <= 30))[:final_limit]


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
        prefix_pattern = re.compile(rf"\b([a-z0-9]+)\s+{re.escape(school)}\b")
        match = prefix_pattern.search(text)
        allowed_prefixes = {
            "the",
            "a",
            "an",
            "at",
            "of",
            "for",
            "chapter",
            "fraternity",
            "sorority",
            "greek",
            "life",
            *(_fraternity_tokens(job.fraternity_slug) or []),
            *(_fraternity_tokens(_display_name(job.fraternity_slug)) or []),
        }
        if match and match.group(1) not in allowed_prefixes:
            return True
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
    if _school_has_conflicting_signal(job, text):
        return False
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
        if _school_has_conflicting_signal(job, combined):
            return False
        handle_score = _instagram_handle_match_score(instagram_url, job)
        if handle_score >= 4:
            return True
        if _search_result_is_relevant(job, result):
            return True
        combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
        if handle_score >= 3 and (_school_matches(job, combined) or _chapter_matches(job, combined)):
            return True
        return False

    combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
    mentions_instagram = any(marker in combined for marker in ("instagram", "insta", "ig ", " ig"))
    if not mentions_instagram:
        return False
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))

def _should_fetch_search_result_page(job: FieldJob, result: SearchResult, target: str) -> bool:
    if target == "instagram":
        result_host = (urlparse(result.url).netloc or "").lower()
        if result_host in {"instagram.com", "www.instagram.com"} or result_host.endswith(".instagram.com"):
            return False
        if _website_trust_tier(job, result.url) == "tier1":
            return True
        lowered = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
        if any(
            marker in lowered
            for marker in (
                "instagram",
                "chapter",
                "student organization",
                "student org",
                "fraternity",
                "sorority",
                "greek life",
                "ifc",
            )
        ):
            return True
        website_host = (urlparse(_current_website_url(job) or "").netloc or "").lower()
        if website_host and (result_host == website_host or result_host.endswith(f".{website_host}")):
            return True
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
    identity_email = _email_local_part_has_identity(email, job)
    generic_office_email = _email_local_part_looks_generic_office(email)
    domain = _email_domain(email)
    domain_matches = _email_domain_matches_known_school_or_website(job, domain)

    if document is None:
        if generic_office_email and not identity_email:
            return False
        if identity_email and (domain_matches or _fraternity_matches(job, normalized_email)):
            return True
        return bool(_fraternity_matches(job, normalized_email) and _school_matches(job, normalized_email))

    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or "", email] if part))
    if _school_has_conflicting_signal(job, combined):
        return False

    school_match = _school_matches(job, combined)
    fraternity_match = _fraternity_matches(job, combined)
    chapter_match = _chapter_matches(job, combined)
    strong_contact_context = _email_document_has_contact_context(document) and (school_match or chapter_match)
    tier1_school_page = _website_trust_tier(job, document.url or "") == "tier1"

    if generic_office_email and not identity_email:
        return bool(document.provider == "chapter_website" and fraternity_match and strong_contact_context)

    if document.provider == "provenance" and domain_matches and not generic_office_email and not _website_document_has_conflicting_org_signal(job, document):
        return True

    if tier1_school_page and document.provider != "provenance":
        if not (fraternity_match or chapter_match or identity_email):
            return False
        if generic_office_email and not chapter_match and not identity_email:
            return False

    if identity_email and (school_match or chapter_match or fraternity_match):
        return True
    if fraternity_match and (school_match or chapter_match) and domain_matches:
        return True
    if strong_contact_context and domain_matches and not generic_office_email:
        return True
    return False


def _document_match_text(document: SearchDocument, *, limit: int = 1600) -> str:
    return _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:limit], document.url or ""] if part))


def _website_document_looks_low_signal(document: SearchDocument) -> bool:
    combined = _document_match_text(document, limit=900)
    return any(marker in combined for marker in _LOW_SIGNAL_AFFILIATION_MARKERS)


def _website_candidate_looks_low_signal(url: str, document: SearchDocument | None = None) -> bool:
    parsed = urlparse(url)
    path_text = _normalized_match_text(f"{parsed.netloc} {parsed.path} {parsed.query}")
    if ("google." in (parsed.netloc or "").lower() and ("/maps/" in (parsed.path or "").lower() or "/maps/d/" in (parsed.path or "").lower())) or "forcekml" in path_text or (parsed.path or "").lower().endswith(".kml"):
        return True
    if any(marker in path_text for marker in ("archive", "archives", "digital api collection", " download")):
        return True
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


def _verify_official_website_candidate(job: FieldJob, candidate_url: str, document: SearchDocument) -> PrecisionDecision:
    return tool_official_domain_verifier(
        candidate_url=candidate_url,
        fraternity_name=_display_name(job.fraternity_slug),
        fraternity_slug=job.fraternity_slug or "",
        chapter_name=job.chapter_name,
        university_name=job.university_name or str(job.payload.get("candidateSchoolName") or ""),
        source_url=job.source_base_url,
        document_url=document.url,
        document_title=document.title or "",
        document_text=document.text,
        document_html=document.html or "",
    )


def _search_page_link_has_website_context(job: FieldJob, document: SearchDocument, candidate_url: str) -> bool:
    if document.provider != "search_page" or not document.html:
        return False
    document_url = document.url or ""
    if not document_url:
        return False
    candidate_normalized = _normalize_url(urljoin(document_url, candidate_url))
    soup = _parse_document_markup(document.html)
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

    school_initials = _school_initials(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    school_signal = bool(school_initials and len(school_initials) >= 3 and school_initials in local_part)
    if school_signal:
        return True

    chapter_tokens = _significant_tokens(job.chapter_name)
    chapter_signal = any(token in local_part for token in chapter_tokens if len(token) >= 4)
    if chapter_signal:
        return True

    if fraternity_initials and len(fraternity_initials) >= 3 and fraternity_initials in local_part:
        return True
    return False


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

def _instagram_handle_text(instagram_url: str) -> str:
    normalized = (instagram_url or "").strip().rstrip("/")
    if not normalized:
        return ""
    return _compact_text(normalized.rsplit("/", 1)[-1])


def _instagram_handle_match_score(instagram_url: str, job: FieldJob) -> int:
    handle = _instagram_handle_text(instagram_url)
    score = 0
    fraternity_compact = _compact_text(_display_name(job.fraternity_slug))
    fraternity_initials = _initialism(_display_name(job.fraternity_slug))
    fraternity_aliases = [
        _compact_text(alias)
        for alias in _fraternity_query_aliases(_display_name(job.fraternity_slug), job.fraternity_slug)
        if alias
    ]
    school_tokens = _significant_tokens(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    school_initials = _school_initials(job.university_name or str(job.payload.get("candidateSchoolName") or ""))
    chapter_compact = _compact_text(job.chapter_name)
    chapter_initials = _initialism(job.chapter_name)

    if fraternity_compact and fraternity_compact in handle:
        score += 2
    elif any(alias and len(alias) >= 3 and alias in handle for alias in fraternity_aliases):
        score += 2
    elif fraternity_initials and len(fraternity_initials) >= 3 and fraternity_initials in handle:
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
    handle = _instagram_handle_text(instagram_url)
    fraternity_compact = _compact_text(_display_name(job.fraternity_slug))
    fraternity_initials = _initialism(_display_name(job.fraternity_slug))
    fraternity_aliases = [
        _compact_text(alias)
        for alias in _fraternity_query_aliases(_display_name(job.fraternity_slug), job.fraternity_slug)
        if alias
    ]
    if fraternity_compact and fraternity_compact in handle:
        return True
    if any(alias and len(alias) >= 3 and alias in handle for alias in fraternity_aliases):
        return True
    return bool(fraternity_initials and len(fraternity_initials) >= 3 and fraternity_initials in handle)


def _instagram_handle_has_local_identity(instagram_url: str, job: FieldJob) -> bool:
    handle = _instagram_handle_text(instagram_url)
    if not handle:
        return False

    school = job.university_name or str(job.payload.get("candidateSchoolName") or "")
    school_initials = _school_initials(school)
    if school_initials and len(school_initials) >= 3 and school_initials.lower() in handle:
        return True

    school_tokens = _significant_tokens(school)
    if any(token in handle for token in school_tokens if len(token) >= 5):
        return True

    if _has_nongeneric_chapter_signal(job):
        chapter_compact = _compact_text(job.chapter_name)
        chapter_initials = _initialism(job.chapter_name)
        if chapter_compact and len(chapter_compact) >= 4 and chapter_compact in handle:
            return True
        if chapter_initials and len(chapter_initials) >= 2 and chapter_initials.lower() in handle:
            return True

    return False


def _instagram_handle_looks_like_school_brand(instagram_url: str, job: FieldJob) -> bool:
    handle = _instagram_handle_text(instagram_url)
    if not handle:
        return False

    school = job.university_name or str(job.payload.get("candidateSchoolName") or "")
    school_compact = _compact_text(school)
    school_initials = _school_initials(school)
    if school_compact:
        if handle == school_compact or handle.startswith(school_compact):
            return True
        if len(handle) >= 5 and school_compact.startswith(handle):
            return True
    return bool(school_initials and len(school_initials) >= 3 and handle == school_initials.lower())


def _instagram_handle_looks_national_generic(instagram_url: str, job: FieldJob) -> bool:
    handle = _instagram_handle_text(instagram_url)
    if not handle:
        return False
    if not _instagram_handle_has_fraternity_token(instagram_url, job):
        return False
    if _instagram_handle_has_local_identity(instagram_url, job):
        return False
    return any(marker in handle for marker in _NATIONAL_GENERIC_INSTAGRAM_MARKERS)


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
    handle = _instagram_handle_text(instagram_url)
    fraternity_compact = _compact_text(_display_name(job.fraternity_slug))
    if not fraternity_compact:
        return False
    remainder = handle.removeprefix(fraternity_compact) if handle.startswith(fraternity_compact) else handle
    return handle == fraternity_compact or len(remainder) < 3


def _instagram_looks_institutional_or_directory_account(instagram_url: str, document: SearchDocument) -> bool:
    handle = _instagram_handle_text(instagram_url)
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
        "fsl",
        "ifc fraternities",
        "recognized chapters",
        "fraternity chapters",
        "fraternities",
        "fraternity student life",
        "greek life",
        "greek organizations",
        "chapters",
    )
    return any(marker in lowered for marker in markers)


def _instagram_document_is_relevant(job: FieldJob, document: SearchDocument) -> bool:
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or ""] if part))
    if _school_has_conflicting_signal(job, combined):
        return False
    if document.provider == "chapter_website":
        if any(sanitize_as_instagram(link) for link in document.links):
            return True
        if _INSTAGRAM_RE.search(document.text) or _INSTAGRAM_HANDLE_HINT_RE.search(document.text) or _INSTAGRAM_NEARBY_HANDLE_RE.search(document.text):
            return True
        chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
        return _fraternity_matches(job, combined) and (_school_matches(job, combined) or chapter_match)
    if document.provider in {"provenance", "nationals_directory"}:
        if any(sanitize_as_instagram(link) for link in document.links):
            return True
        if _INSTAGRAM_RE.search(document.text) or _INSTAGRAM_HANDLE_HINT_RE.search(document.text) or _INSTAGRAM_NEARBY_HANDLE_RE.search(document.text):
            return True
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
            if handle_score >= 3 and (_school_matches(job, combined) or chapter_match):
                return True
        return False
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
    if _instagram_handle_looks_national_generic(instagram_url, job):
        return False
    school_brand_handle = _instagram_handle_looks_like_school_brand(instagram_url, job)
    if handle_score >= 4 and not school_brand_handle:
        return True
    if (
        handle_score >= 3
        and not school_brand_handle
        and _has_nongeneric_chapter_signal(job)
        and _instagram_handle_has_fraternity_token(instagram_url, job)
    ):
        return True
    if document is None:
        return False
    combined = _instagram_candidate_text(document, instagram_url)
    chapter_designation = _chapter_designation_signal(job, combined)
    if _school_has_conflicting_signal(job, combined):
        return False
    if _instagram_has_conflicting_org_signal(job, combined) and handle_score < 5:
        return False
    fraternity_match = _fraternity_matches(job, combined)
    chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
    local_identity = _instagram_handle_has_local_identity(instagram_url, job)
    school_match = _school_matches(job, combined)
    effective_local_identity = local_identity and not school_brand_handle
    if school_brand_handle and school_match and not (fraternity_match or chapter_match or effective_local_identity or chapter_designation > 0):
        return False
    if document.provider == "chapter_website":
        if _instagram_looks_institutional_or_directory_account(instagram_url, document):
            return False
        if _website_trust_tier(job, document.url or "") == "tier1" and not (effective_local_identity or fraternity_match or chapter_match or _chapter_designation_signal(job, combined) > 0):
            return False
        if effective_local_identity:
            return True
        return fraternity_match and (school_match or chapter_match)
    if document.provider == "source_page":
        if _instagram_looks_institutional_or_directory_account(instagram_url, document):
            return False
        if school_brand_handle and not (effective_local_identity or chapter_match or chapter_designation > 0):
            return False
        if not (effective_local_identity or chapter_match or chapter_designation > 0):
            return False
        if chapter_match:
            return True
        if effective_local_identity and (school_match or chapter_designation > 0):
            return True
        return fraternity_match and (school_match or chapter_designation > 0)
    if document.provider in {"provenance", "nationals_directory"}:
        if chapter_match:
            return True
        if handle_score >= 4:
            return True
        if effective_local_identity and _instagram_handle_has_fraternity_token(instagram_url, job):
            return True
        if handle_score >= 2 and _instagram_handle_has_fraternity_token(instagram_url, job) and school_match:
            return True
        if handle_score >= 3 and school_match:
            return True
        return False
    if _website_trust_tier(job, document.url or "") == "tier1":
        if _instagram_looks_institutional_or_directory_account(instagram_url, document):
            return False
        if school_brand_handle and not (effective_local_identity or chapter_match or chapter_designation > 0):
            return False
        if not (_instagram_handle_has_fraternity_token(instagram_url, job) or effective_local_identity or chapter_match or chapter_designation > 0):
            return False
        return school_match and (
            fraternity_match
            or chapter_match
            or chapter_designation > 0
            or effective_local_identity
        )
    return fraternity_match and (school_match or chapter_match)


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



def _canonical_school_name(value: object) -> str:
    school = str(value or "").strip()
    if not school:
        return ""
    for delimiter in (" - ", " | ", " / "):
        if delimiter in school:
            left, right = school.split(delimiter, 1)
            normalized_left = str(left).strip()
            normalized_right = str(right).strip()
            if _looks_like_institution_name(normalized_left) and _looks_like_chapterish_school_suffix(normalized_right):
                school = normalized_left
                break
    if " (" in school and school.endswith(")"):
        prefix, suffix = school.rsplit(" (", 1)
        normalized_prefix = prefix.strip()
        normalized_suffix = suffix[:-1].strip()
        if _looks_like_institution_name(normalized_prefix) and _looks_like_chapterish_school_suffix(normalized_suffix):
            school = normalized_prefix
    return school


def _looks_like_institution_name(value: str) -> bool:
    normalized = _normalized_match_text(value)
    return any(marker in normalized for marker in _INSTITUTION_NAME_MARKERS)


def _looks_like_chapterish_school_suffix(value: str) -> bool:
    normalized = _normalized_match_text(value)
    if not normalized:
        return False
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        return False
    if len(tokens) <= 4 and all(token in _GREEK_LETTER_TOKENS or token in _CHAPTER_SIGNAL_STOPWORDS for token in tokens):
        return True
    if len(tokens) <= 5 and any(token in _GREEK_LETTER_TOKENS for token in tokens):
        return True
    return False


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
    soup = _parse_document_markup(document.html)
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
    block_text_lower = block_text.lower()
    website_label_present = "website" in block_text_lower
    national_website_label = any(marker in block_text_lower for marker in ("national website", "international website", "headquarters website", "hq website"))

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
        if website_label_present and not national_website_label and is_external and link.lower().startswith(("http://", "https://")) and not website_url:
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

    if not website_url and website_label_present and not national_website_label:
        website_pattern = re.search(r"website\s*:\s*(https?://[^\s]+)", block_text, flags=re.IGNORECASE)
        if website_pattern:
            website_url = website_pattern.group(1).rstrip(".,;)")

    if not website_url and not national_website_label:
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
    website_url = _current_website_url(job)
    if not website_url:
        return False
    if not _field_value_is_confident(job, "website_url"):
        return False
    normalized_url = _normalize_url(website_url)
    if normalized_url.endswith(_DOCUMENT_URL_EXTENSIONS):
        return False
    path_text = _normalized_match_text(website_url)
    if any(marker in path_text for marker in ("archive", "archives", "download", "digital")):
        return False
    if _candidate_is_source_domain(website_url, job):
        return False
    if _website_candidate_looks_low_signal(website_url):
        return False
    return True


def _field_value_is_confident(job: FieldJob, field_name: str) -> bool:
    state = str((job.field_states or {}).get(field_name) or "").strip().lower()
    if state in {"low_confidence", "missing", "inactive", "confirmed_absent", "invalid_entity"}:
        return False
    return True



















