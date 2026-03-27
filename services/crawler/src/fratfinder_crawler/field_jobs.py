from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import (
    FieldJob,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_TO_STATE_KEY,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    ProvenanceRecord,
    ReviewItemCandidate,
)
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
_BLOCKED_WEBSITE_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "facebook.com", "www.facebook.com", "instagram.com", "www.instagram.com", "twitter.com", "x.com", "youtube.com", "www.youtube.com", "linkedin.com", "www.linkedin.com", "bing.com", "www.bing.com", "stackoverflow.com", "www.stackoverflow.com", "stackexchange.com", "github.com", "www.github.com", "sigmaaldrich.com", "www.sigmaaldrich.com", "sigma-aldrich.com", "www.sigma-aldrich.com", "milliporesigma.com", "www.milliporesigma.com", "merckmillipore.com", "www.merckmillipore.com"}
_TIER2_WEBSITE_HOSTS = {"linktr.ee", "www.linktr.ee", "beacons.ai", "www.beacons.ai", "bio.site", "www.bio.site", "campsite.bio", "www.campsite.bio", "allmylinks.com", "www.allmylinks.com", "lnk.bio", "www.lnk.bio", "stan.store", "www.stan.store"}
_LOW_SIGNAL_INSTAGRAM_RESULT_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "dcurbanmom.com", "www.dcurbanmom.com", "worldscholarshipforum.com", "www.worldscholarshipforum.com", "sigmaaldrich.com", "www.sigmaaldrich.com", "sigma-aldrich.com", "www.sigma-aldrich.com", "milliporesigma.com", "www.milliporesigma.com", "merckmillipore.com", "www.merckmillipore.com"}
_LOW_SIGNAL_EMAIL_RESULT_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "facebook.com", "www.facebook.com", "instagram.com", "www.instagram.com", "x.com", "twitter.com", "www.twitter.com", "youtube.com", "www.youtube.com", "sigmaaldrich.com", "www.sigmaaldrich.com", "sigma-aldrich.com", "www.sigma-aldrich.com", "milliporesigma.com", "www.milliporesigma.com", "merckmillipore.com", "www.merckmillipore.com"}
_FREE_EMAIL_DOMAINS = {"gmail.com", "googlemail.com", "yahoo.com", "outlook.com", "hotmail.com", "live.com", "aol.com", "icloud.com", "me.com", "protonmail.com"}
_MATCH_STOPWORDS = {"university", "college", "campus", "chapter", "official", "site", "email", "contact", "instagram", "profile", "fraternity", "house", "the", "and", "for"}
_GREEK_LETTER_TOKENS = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega"}


@dataclass(slots=True)
class FieldJobResult:
    chapter_updates: dict[str, str]
    completed_payload: dict[str, str]
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
        field_name: str | None = None,
    ):
        self._repository = repository
        self._logger = logger
        self._worker_id = worker_id
        self._base_backoff_seconds = max(1, base_backoff_seconds)
        self._source_slug = source_slug
        self._head_requester = head_requester or requests.head
        self._get_requester = get_requester or requests.get
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
        self._field_name = field_name
        self._search_errors_encountered = False
        self._search_queries_attempted = 0
        self._search_queries_failed = 0
        self._search_queries_succeeded = 0
        self._search_fanout_aborted = False
        self._search_result_cache: dict[str, list[SearchResult]] = {}
        self._search_document_cache: dict[str, SearchDocument | None] = {}

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
                    )
                    continue

                backoff_seconds = exc.backoff_seconds if exc.backoff_seconds is not None else self._base_backoff_seconds * (2 ** (job.attempts - 1))
                self._repository.requeue_field_job(job, str(exc), backoff_seconds, preserve_attempt=exc.preserve_attempt)
                requeued += 1
                log_event(
                    self._logger,
                    "field_job_requeued",
                    field_job_id=job.id,
                    chapter_slug=job.chapter_slug,
                    field_name=job.field_name,
                    backoff_seconds=backoff_seconds,
                    error=str(exc),
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

    def _process_single_job(self, job: FieldJob) -> FieldJobResult:
        self._search_errors_encountered = False
        self._search_queries_attempted = 0
        self._search_queries_failed = 0
        self._search_queries_succeeded = 0
        self._search_fanout_aborted = False

        if job.field_name == FIELD_JOB_FIND_EMAIL:
            if job.contact_email:
                return self._already_populated_result(job.field_name, job.contact_email)
            match = self._find_email_candidate(job)
            if match is None:
                raise self._no_candidate_error(job, "No candidate email found in provenance, chapter website, or search results")
            return self._candidate_result(job, match, "contact_email")

        if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
            if job.instagram_url:
                return self._already_populated_result(job.field_name, job.instagram_url)
            match = self._find_instagram_candidate(job)
            if match is not None:
                return self._candidate_result(job, match, "instagram_url")
            fallback_result = self._resolve_instagram_search_miss(job)
            if fallback_result is not None:
                return fallback_result
            raise self._no_candidate_error(job, "No candidate instagram URL found in provenance, chapter website, or search results")

        if job.field_name == FIELD_JOB_FIND_WEBSITE:
            if job.website_url:
                return self._already_populated_result(job.field_name, job.website_url)
            match = self._find_website_candidate(job)
            if match is None:
                raise self._no_candidate_error(job, "No candidate website URL available")
            return self._candidate_result(job, match, "website_url")

        if job.field_name == FIELD_JOB_VERIFY_WEBSITE:
            return self._verify_website(job)

        if job.field_name == FIELD_JOB_VERIFY_SCHOOL:
            return self._verify_school_match(job)

        raise RetryableJobError(f"Unsupported field job type: {job.field_name}")

    def _candidate_result(self, job: FieldJob, match: CandidateMatch, target_field: str) -> FieldJobResult:
        write_threshold = self._write_threshold(job, target_field, match)
        if match.confidence < write_threshold:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={
                    "status": "review_required",
                    "value": match.value,
                    "confidence": f"{match.confidence:.2f}",
                    "source_url": match.source_url,
                },
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
                    },
                ),
            )

        found_threshold = self._found_threshold(job, target_field, match)
        field_state = "found" if match.confidence >= found_threshold else "low_confidence"
        chapter_updates = {target_field: match.value}
        field_state_updates = {target_field: field_state}
        if target_field != "website_url" and match.related_website_url and _is_safe_related_website_url(job, match.related_website_url):
            if not job.website_url or job.website_url == match.related_website_url:
                chapter_updates["website_url"] = match.related_website_url
                field_state_updates["website_url"] = "found" if match.confidence >= found_threshold else "low_confidence"

        completed_payload = {
            "status": "updated",
            target_field: match.value,
            "confidence": f"{match.confidence:.2f}",
            "source_url": match.source_url,
        }
        if match.query:
            completed_payload["query"] = match.query
        if match.related_website_url and target_field != "website_url":
            completed_payload["related_website_url"] = match.related_website_url

        provenance_records = [
            ProvenanceRecord(
                source_slug=job.source_slug or job.payload.get("sourceSlug") or "search-enrichment",
                source_url=match.source_url,
                field_name=target_field,
                field_value=match.value,
                source_snippet=match.source_snippet[:400],
                confidence=match.confidence,
            )
        ]
        if target_field != "website_url" and chapter_updates.get("website_url"):
            provenance_records.append(
                ProvenanceRecord(
                    source_slug=job.source_slug or job.payload.get("sourceSlug") or "search-enrichment",
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
        if not job.website_url:
            raise RetryableJobError("No website URL available to verify")

        try:
            response = self._head_requester(job.website_url, timeout=10, allow_redirects=True)
        except requests.Timeout as exc:
            raise RetryableJobError("Website verification timed out") from exc
        except requests.RequestException as exc:
            raise RetryableJobError(f"Website verification request failed: {exc}") from exc

        status_code = getattr(response, "status_code", None)
        if status_code is None:
            raise RetryableJobError("Website verification did not return an HTTP status code")
        if 200 <= status_code < 400:
            return FieldJobResult(
                chapter_updates={},
                completed_payload={"status": "verified", "website_url": job.website_url, "status_code": str(status_code)},
                field_state_updates={"website_url": "found"},
            )
        if 400 <= status_code < 500:
            raise RetryableJobError(f"Website verification returned client error status {status_code}")
        raise RetryableJobError(f"Website verification returned server error status {status_code}")

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
        raise RetryableJobError("Insufficient school data to verify school match")

    def _find_email_candidate(self, job: FieldJob) -> CandidateMatch | None:
        matches: list[CandidateMatch] = []

        provenance_match = self._find_email_candidate_from_provenance(job)
        if provenance_match is not None:
            matches.append(provenance_match)

        website_matches = self._extract_email_matches_from_website(job)
        matches.extend(website_matches)
        best_local = _best_match(matches)
        if best_local is not None and best_local.confidence >= self._found_threshold(job, "contact_email", best_local):
            return best_local

        if not _website_is_confident(job):
            trusted_school_matches = self._extract_email_matches_from_trusted_school_pages(job)
            matches.extend(trusted_school_matches)
            best_school = _best_match(matches)
            if best_school is not None and best_school.confidence >= self._found_threshold(job, "contact_email", best_school):
                return best_school

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

        if job.website_url and _website_trust_tier(job, job.website_url) == "tier1":
            website_document = self._fetch_search_document(job.website_url, provider="chapter_website")
            if website_document is not None:
                website_matches = self._extract_instagram_matches(website_document, job)
                matches.extend(website_matches)
                best_website = _best_match(website_matches)
                if best_website is not None and best_website.confidence >= self._found_threshold(job, "instagram_url", best_website):
                    return best_website

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

        provenance_match = self._find_instagram_candidate_from_provenance(job)
        if provenance_match is not None:
            matches.append(provenance_match)

        return _best_match(matches)

    def _find_email_candidate_from_provenance(self, job: FieldJob) -> CandidateMatch | None:
        provenance_document = SearchDocument(text=self._source_text(job), provider="provenance", url=job.source_base_url)
        return _best_match(self._extract_email_matches(provenance_document, job))

    def _extract_email_matches_from_website(self, job: FieldJob) -> list[CandidateMatch]:
        if not job.website_url:
            return []

        homepage_document = self._fetch_search_document(job.website_url, provider="chapter_website")
        if homepage_document is None:
            return []

        matches = self._extract_email_matches(homepage_document, job)
        followup_links = _email_followup_links(homepage_document, job.website_url, limit=self._max_search_pages)
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
        matches.extend(self._extract_website_matches(provenance_document, job))
        for document in self._search_documents(job, target="website_school", include_existing=False):
            matches.extend(self._extract_website_matches(document, job))
        best = _best_match(matches)
        if best is not None:
            return best
        for document in self._search_documents(job, target="website_fallback", include_existing=False):
            matches.extend(self._extract_website_matches(document, job))
        return _best_match(matches)

    def _extract_email_matches(self, document: SearchDocument, job: FieldJob) -> list[CandidateMatch]:
        if not _document_is_relevant(job, document):
            return []
        matches: list[CandidateMatch] = []
        query = document.query
        for link in document.links:
            mailto_match = _MAILTO_RE.search(link)
            if mailto_match:
                email = unquote(mailto_match.group(1))
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
            confidence = self._score_email_candidate(email, document, job, from_mailto=False)
            if not self._email_search_candidate_passes_gate(email, document, job):
                continue
            matches.append(
                CandidateMatch(
                    value=email,
                    confidence=confidence,
                    source_url=document.url or (job.website_url or job.source_base_url or "search-enrichment"),
                    source_snippet=document.text[:400],
                    field_name="contact_email",
                    source_provider=document.provider,
                    related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                    query=document.query,
                )
            )
        deobfuscated = _deobfuscate_emails(document.text)
        for email in _EMAIL_RE.findall(deobfuscated):
            confidence = self._score_email_candidate(email, document, job, from_mailto=False, obfuscated=True)
            if not self._email_search_candidate_passes_gate(email, document, job):
                continue
            matches.append(
                CandidateMatch(
                    value=email,
                    confidence=confidence,
                    source_url=document.url or (job.website_url or job.source_base_url or "search-enrichment"),
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
            normalized = _normalize_instagram_candidate(link)
            if normalized:
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
            normalized = _normalize_instagram_candidate(match)
            if normalized:
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
            normalized = _normalize_instagram_candidate(handle_match.group(1))
            if normalized:
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
            normalized = _normalize_instagram_candidate(nearby_match.group(1))
            if normalized:
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

        if not _email_looks_relevant_to_job(email, job, document=document):
            confidence -= 0.24

        return max(0.0, min(0.95, confidence))
    def _score_instagram_candidate(self, instagram_url: str, document: SearchDocument, job: FieldJob, *, direct_url: bool) -> float:
        provider_base = {
            "provenance": 0.8,
            "chapter_website": 0.9,
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
        if include_existing:
            source_text = self._source_text(job)
            if source_text:
                documents.append(SearchDocument(text=source_text, provider="provenance", url=job.source_base_url))
            if job.website_url:
                website_document = self._fetch_search_document(job.website_url, provider="chapter_website")
                if website_document is not None:
                    documents.append(website_document)

        if self._search_fanout_aborted:
            return documents

        seen_urls: set[str] = set()
        fetched_pages = 0
        for query in self._build_search_queries(job, target):
            query_results = self._run_search(query)
            if self._search_queries_succeeded == 0 and self._search_queries_failed > 0:
                self._search_fanout_aborted = True
                log_event(
                    self._logger,
                    "search_query_fanout_aborted",
                    chapter_slug=job.chapter_slug,
                    field_name=job.field_name,
                    reason="provider_unavailable",
                    attempted_queries=self._search_queries_attempted,
                )
                break

            for result in query_results:
                if not _search_result_is_useful(job, result, target):
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
                        [quoted_fraternity or fraternity, university, "chapter profile", f"site:{domain}"],
                        [university, quoted_fraternity or fraternity, "fraternity", f"site:{domain}"],
                    ]
                )
            query_parts.extend(
                [
                    [quoted_fraternity or fraternity, university, "student organization", "site:.edu"],
                    [quoted_fraternity or fraternity, university, "greek life", "site:.edu"],
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
                ]
            )
        elif target == "email":
            website_host = (urlparse(job.website_url or "").netloc or "").lower()
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
            log_event(self._logger, "search_query_cache_hit", query=query, result_count=len(cached_results))
            return list(cached_results)

        self._search_queries_attempted += 1
        try:
            results = self._search_client.search(query)
        except SearchUnavailableError as exc:
            self._search_errors_encountered = True
            self._search_queries_failed += 1
            log_event(self._logger, "search_unavailable", level=30, query=query, error=str(exc))
            return []
        except requests.RequestException as exc:
            self._search_errors_encountered = True
            self._search_queries_failed += 1
            log_event(self._logger, "search_request_failed", level=30, query=query, error=str(exc))
            return []

        self._search_queries_succeeded += 1
        self._search_result_cache[query] = list(results)
        log_event(self._logger, "search_query_executed", query=query, result_count=len(results))
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
        )
        return SearchDocument(text=text, links=links, url=url, title=title, provider=provider, query=query)

    def _already_populated_result(self, field_name: str, value: str) -> FieldJobResult:
        state_key = FIELD_JOB_TO_STATE_KEY[field_name]
        return FieldJobResult(
            chapter_updates={},
            completed_payload={"status": "already_populated", "value": value},
            field_state_updates={state_key: "found"},
        )

    def _source_text(self, job: FieldJob) -> str:
        snippets = self._repository.fetch_provenance_snippets(job.chapter_id)
        return "\n".join(snippets)

    def _requires_website_first(self, job: FieldJob) -> bool:
        return False

    def _write_threshold(self, job: FieldJob, target_field: str, match: CandidateMatch) -> float:
        if target_field == "website_url":
            candidate_tier = _website_trust_tier(job, match.value)
            source_tier = _website_trust_tier(job, match.source_url)
            if candidate_tier == "tier2":
                return 1.0
            if self._search_provider == "bing_html":
                if candidate_tier == "tier1" or source_tier == "tier1":
                    return 0.88
                if match.source_provider == "search_result":
                    return 0.98
                return 0.96
        if self._search_provider == "bing_html" and match.source_provider in {"search_result", "search_page"}:
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
            if self._search_provider == "bing_html" and (candidate_tier == "tier1" or source_tier == "tier1"):
                return 0.90
        if self._search_provider == "bing_html" and match.source_provider in {"search_result", "search_page"}:
            return {
                "website_url": 0.95,
                "contact_email": 0.92,
                "instagram_url": 0.90,
            }.get(target_field, 0.90)
        return 0.85

    def _no_candidate_error(self, job: FieldJob, message: str) -> RetryableJobError:
        if self._search_errors_encountered:
            all_queries_failed = self._search_queries_attempted > 0 and self._search_queries_failed >= self._search_queries_attempted
            if all_queries_failed:
                return RetryableJobError(
                    f"{message}; search provider or network unavailable",
                    backoff_seconds=max(self._dependency_wait_seconds, self._base_backoff_seconds),
                    preserve_attempt=True,
                )
            return RetryableJobError(message, backoff_seconds=self._base_backoff_seconds)

        low_signal = self._search_provider == "bing_html" and job.field_name == FIELD_JOB_FIND_WEBSITE
        if low_signal:
            # Park unresolved website jobs instead of terminal-failing; this keeps discovery
            # resilient when search signals are weak or temporarily noisy.
            return RetryableJobError(
                message,
                backoff_seconds=max(self._negative_result_cooldown_seconds, self._base_backoff_seconds),
                low_signal=True,
                preserve_attempt=True,
            )
        return RetryableJobError(message, backoff_seconds=self._negative_result_cooldown_seconds, low_signal=False)

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

        if fraternity_match and (school_match or chapter_match):
            return True
        if source_tier == "tier1" and school_match and (fraternity_match or chapter_match):
            return True
        if source_tier == "tier1" and school_match and _email_domain_matches_known_school_or_website(job, email_domain):
            return True
        if overlap_score >= 4 and (school_match or fraternity_match):
            return True
        if _email_domain_matches_known_school_or_website(job, email_domain) and (school_match or fraternity_match):
            return True
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
        if _chapter_designation_signal(job, combined) < 0:
            return False
        if handle_score >= 4 and (school_match or fraternity_match):
            return True
        if handle_score >= 3 and school_match and fraternity_match:
            return True
        if overlap_score >= 4 and school_match and fraternity_match:
            return True
        if overlap_score >= 5 and school_match and (fraternity_match or chapter_match):
            return True
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
        if document.provider != "provenance" and not _document_is_relevant(job, document):
            return []
        matches: list[CandidateMatch] = []
        candidates = list(document.links)
        candidates.extend(_URL_RE.findall(document.text))
        seen: set[str] = set()
        for candidate in candidates:
            url = candidate.strip().rstrip('.,;)')
            if not url.lower().startswith("http"):
                continue
            if _is_disallowed_website_candidate(url):
                continue
            if job.source_base_url and _normalize_url(url) == _normalize_url(job.source_base_url):
                continue
            key = _normalize_url(url)
            if key in seen:
                continue
            seen.add(key)
            confidence = self._score_website_candidate(url, document, job)
            if confidence < 0.65:
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
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.startswith("@"):
        candidate = candidate[1:]
    if not candidate.lower().startswith("http"):
        if "instagram.com/" in candidate.lower():
            candidate = f"https://{candidate.lstrip('/')}"
        else:
            candidate = f"https://www.instagram.com/{candidate}"

    match = _INSTAGRAM_PATH_RE.search(candidate)
    if not match:
        return None
    handle = match.group(1).strip("/")
    handle = handle.split("/")[0].split("?")[0].split("#")[0]
    handle = handle.lstrip("@")
    if not handle or handle.lower() in _IGNORED_INSTAGRAM_SEGMENTS:
        return None
    return f"https://www.instagram.com/{handle}"





def _is_generic_greek_letter_chapter_name(value: str | None) -> bool:
    tokens = _normalized_match_text(value).split()
    return bool(tokens) and len(tokens) <= 4 and all(token in _GREEK_LETTER_TOKENS for token in tokens)


def _normalized_match_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _compact_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


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


def _fraternity_tokens(value: str | None) -> list[str]:
    return [token for token in _normalized_match_text(value).split() if len(token) >= 3 and token not in {"the", "and", "of", "for"}]


def _fraternity_matches(job: FieldJob, text: str) -> bool:
    fraternity_display = _display_name(job.fraternity_slug)
    fraternity_phrase = _normalized_match_text(fraternity_display)
    compact_text = _compact_text(text)
    fraternity_compact = _compact_text(fraternity_display)
    if fraternity_phrase and fraternity_phrase in text:
        return True
    if fraternity_compact and fraternity_compact in compact_text:
        return True
    tokens = _fraternity_tokens(fraternity_display)
    if not tokens:
        return False
    required = len(tokens) if len(tokens) <= 2 else 2
    return sum(1 for token in tokens if token in text) >= required


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


def _chapter_matches(job: FieldJob, text: str) -> bool:
    chapter_tokens = _significant_tokens(job.chapter_name)
    if not chapter_tokens:
        chapter_tokens = _significant_tokens(job.chapter_slug)
    return sum(1 for token in chapter_tokens if token in text) >= 1


def _document_is_relevant(job: FieldJob, document: SearchDocument) -> bool:
    if document.provider in {"provenance", "chapter_website"}:
        return True
    combined = _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or ""] if part))
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))


def _search_result_is_relevant(job: FieldJob, result: SearchResult) -> bool:
    combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or _chapter_matches(job, combined))


def _search_result_is_useful(job: FieldJob, result: SearchResult, target: str) -> bool:
    if target == "email":
        hostname = (urlparse(result.url).netloc or "").lower()
        if hostname in _LOW_SIGNAL_EMAIL_RESULT_HOSTS or any(hostname.endswith(f".{blocked}") for blocked in _LOW_SIGNAL_EMAIL_RESULT_HOSTS):
            return False
        combined = _normalized_match_text(f"{result.title} {result.snippet} {result.url}")
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
        return _instagram_handle_match_score(instagram_url, job) >= 2 or _search_result_is_relevant(job, result)

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
        website_host = (urlparse(job.website_url or "").netloc or "").lower()
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

    website_host = (urlparse(job.website_url or "").netloc or "").lower()
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


def _instagram_candidate_text(document: SearchDocument, instagram_url: str) -> str:
    return _normalized_match_text(" ".join(part for part in [document.title or "", document.text[:1200], document.url or "", instagram_url] if part))


def _has_nongeneric_chapter_signal(job: FieldJob) -> bool:
    return not _is_generic_greek_letter_chapter_name(job.chapter_name) and bool(_significant_tokens(job.chapter_name))


def _normalized_greek_chapter_designation(value: str | None) -> str:
    if not _is_generic_greek_letter_chapter_name(value):
        return ""
    return _normalized_match_text(value)


def _extract_greek_chapter_designations(text: str) -> set[str]:
    greek = "|".join(sorted(_GREEK_LETTER_TOKENS, key=len, reverse=True))
    pattern = re.compile(rf"\b(?:{greek})(?:\s+(?:{greek})){{0,2}}(?=\s+chapter\b)")
    return {match.group(0).strip() for match in pattern.finditer(text)}


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
    if document.provider in {"provenance", "chapter_website"}:
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
    if handle_score >= 3 and _has_nongeneric_chapter_signal(job):
        return True
    if document is None:
        return False
    combined = _instagram_candidate_text(document, instagram_url)
    chapter_match = _has_nongeneric_chapter_signal(job) and _chapter_matches(job, combined)
    if document.provider in {"provenance", "chapter_website"} and (chapter_match or handle_score >= 1):
        return True
    return _fraternity_matches(job, combined) and (_school_matches(job, combined) or chapter_match)


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


def _is_safe_related_website_url(job: FieldJob, url: str) -> bool:
    if job.website_url and _normalize_url(job.website_url) == _normalize_url(url):
        return True
    return not _is_disallowed_website_candidate(url) and _search_result_is_relevant(job, SearchResult(title="", url=url, snippet=url, provider="derived", rank=0))

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



class RetryableJobError(Exception):
    def __init__(
        self,
        message: str,
        *,
        backoff_seconds: int | None = None,
        low_signal: bool = False,
        preserve_attempt: bool = False,
    ):
        super().__init__(message)
        self.backoff_seconds = backoff_seconds
        self.low_signal = low_signal
        self.preserve_attempt = preserve_attempt


def _website_is_confident(job: FieldJob) -> bool:
    if not job.website_url:
        return False
    state = (job.field_states or {}).get("website_url")
    return state != "low_confidence"
















