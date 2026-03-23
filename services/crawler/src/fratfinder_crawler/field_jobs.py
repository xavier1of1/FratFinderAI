from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable
from urllib.parse import unquote, urlparse

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
_IGNORED_INSTAGRAM_SEGMENTS = {"p", "reel", "tv", "stories", "explore", "accounts"}
_BLOCKED_WEBSITE_HOSTS = {"reddit.com", "www.reddit.com", "old.reddit.com", "facebook.com", "www.facebook.com", "instagram.com", "www.instagram.com", "twitter.com", "x.com", "youtube.com", "www.youtube.com", "linkedin.com", "www.linkedin.com", "bing.com", "www.bing.com", "stackoverflow.com", "www.stackoverflow.com", "stackexchange.com", "github.com", "www.github.com"}
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
        max_search_pages: int = 3,
    ):
        self._repository = repository
        self._logger = logger
        self._worker_id = worker_id
        self._base_backoff_seconds = max(1, base_backoff_seconds)
        self._source_slug = source_slug
        self._head_requester = head_requester or requests.head
        self._get_requester = get_requester or requests.get
        self._search_client = search_client
        self._max_search_pages = max(1, max_search_pages)

    def process(self, limit: int = 25) -> dict[str, int]:
        processed = 0
        requeued = 0
        failed_terminal = 0

        for _ in range(limit):
            job = self._repository.claim_next_field_job(self._worker_id, source_slug=self._source_slug)
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
                if job.attempts >= job.max_attempts:
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

                backoff_seconds = self._base_backoff_seconds * (2 ** (job.attempts - 1))
                self._repository.requeue_field_job(job, str(exc), backoff_seconds)
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
        if job.field_name == FIELD_JOB_FIND_EMAIL:
            if job.contact_email:
                return self._already_populated_result(job.field_name, job.contact_email)
            match = self._find_email_candidate(job)
            if match is None:
                raise RetryableJobError("No candidate email found in provenance, chapter website, or search results")
            return self._candidate_result(job, match, "contact_email")

        if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
            if job.instagram_url:
                return self._already_populated_result(job.field_name, job.instagram_url)
            match = self._find_instagram_candidate(job)
            if match is None:
                raise RetryableJobError("No candidate instagram URL found in provenance, chapter website, or search results")
            return self._candidate_result(job, match, "instagram_url")

        if job.field_name == FIELD_JOB_FIND_WEBSITE:
            if job.website_url:
                return self._already_populated_result(job.field_name, job.website_url)
            match = self._find_website_candidate(job)
            if match is None:
                raise RetryableJobError("No candidate website URL available")
            return self._candidate_result(job, match, "website_url")

        if job.field_name == FIELD_JOB_VERIFY_WEBSITE:
            return self._verify_website(job)

        if job.field_name == FIELD_JOB_VERIFY_SCHOOL:
            return self._verify_school_match(job)

        raise RetryableJobError(f"Unsupported field job type: {job.field_name}")

    def _candidate_result(self, job: FieldJob, match: CandidateMatch, target_field: str) -> FieldJobResult:
        if match.confidence < 0.65:
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

        field_state = "found" if match.confidence >= 0.85 else "low_confidence"
        chapter_updates = {target_field: match.value}
        field_state_updates = {target_field: field_state}
        if target_field != "website_url" and match.related_website_url and _is_safe_related_website_url(job, match.related_website_url):
            if not job.website_url or job.website_url == match.related_website_url:
                chapter_updates["website_url"] = match.related_website_url
                field_state_updates["website_url"] = "found" if match.confidence >= 0.85 else "low_confidence"

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
        for document in self._search_documents(job, target="email"):
            matches.extend(self._extract_email_matches(document, job))
        return _best_match(matches)

    def _find_instagram_candidate(self, job: FieldJob) -> CandidateMatch | None:
        matches: list[CandidateMatch] = []
        for document in self._search_documents(job, target="instagram"):
            matches.extend(self._extract_instagram_matches(document, job))
        return _best_match(matches)

    def _find_website_candidate(self, job: FieldJob) -> CandidateMatch | None:
        matches: list[CandidateMatch] = []
        provenance_document = SearchDocument(
            text=self._source_text(job),
            provider="provenance",
            url=job.source_base_url,
        )
        matches.extend(self._extract_website_matches(provenance_document, job))
        for query in self._build_search_queries(job, target="website"):
            for result in self._run_search(query):
                confidence = self._score_website_result(job, result)
                if confidence < 0.65 or _is_disallowed_website_candidate(result.url) or not _search_result_is_relevant(job, result):
                    continue
                matches.append(
                    CandidateMatch(
                        value=result.url,
                        confidence=confidence,
                        source_url=result.url,
                        source_snippet=f"{result.title} {result.snippet}".strip()[:400],
                        field_name="website_url",
                        query=query,
                    )
                )
        for document in self._search_documents(job, target="website"):
            if document.provider == "search_result":
                continue
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
                matches.append(
                    CandidateMatch(
                        value=email,
                        confidence=confidence,
                        source_url=document.url or link,
                        source_snippet=document.text[:400],
                        field_name="contact_email",
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for email in _EMAIL_RE.findall(document.text):
            confidence = self._score_email_candidate(email, document, job, from_mailto=False)
            matches.append(
                CandidateMatch(
                    value=email,
                    confidence=confidence,
                    source_url=document.url or (job.website_url or job.source_base_url or "search-enrichment"),
                    source_snippet=document.text[:400],
                    field_name="contact_email",
                    related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                    query=document.query,
                )
            )
        deobfuscated = _deobfuscate_emails(document.text)
        for email in _EMAIL_RE.findall(deobfuscated):
            confidence = self._score_email_candidate(email, document, job, from_mailto=False, obfuscated=True)
            matches.append(
                CandidateMatch(
                    value=email,
                    confidence=confidence,
                    source_url=document.url or (job.website_url or job.source_base_url or "search-enrichment"),
                    source_snippet=document.text[:400],
                    field_name="contact_email",
                    related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                    query=document.query,
                )
            )
        return matches

    def _extract_instagram_matches(self, document: SearchDocument, job: FieldJob) -> list[CandidateMatch]:
        if not _document_is_relevant(job, document):
            return []
        matches: list[CandidateMatch] = []
        query = document.query
        for link in document.links:
            normalized = _normalize_instagram_candidate(link)
            if normalized:
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=True)
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for match in _INSTAGRAM_RE.findall(document.text):
            normalized = _normalize_instagram_candidate(match)
            if normalized:
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=True)
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for handle_match in _INSTAGRAM_HANDLE_HINT_RE.finditer(document.text):
            normalized = _normalize_instagram_candidate(handle_match.group(1))
            if normalized:
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=False)
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
                        related_website_url=document.url if document.provider in {"chapter_website", "search_page"} else None,
                        query=query,
                    )
                )
        for nearby_match in _INSTAGRAM_NEARBY_HANDLE_RE.finditer(document.text):
            normalized = _normalize_instagram_candidate(nearby_match.group(1))
            if normalized:
                confidence = self._score_instagram_candidate(normalized, document, job, direct_url=False)
                matches.append(
                    CandidateMatch(
                        value=normalized,
                        confidence=confidence,
                        source_url=document.url or normalized,
                        source_snippet=document.text[:400],
                        field_name="instagram_url",
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
            "search_result": 0.7,
            "search_page": 0.83,
        }.get(document.provider, 0.65)
        confidence = provider_base
        if from_mailto:
            confidence += 0.05
        if obfuscated:
            confidence -= 0.04
        confidence += 0.03 * _score_result_context(job, f"{email} {document.title or ''} {document.text[:200]}")
        local_part = email.split("@", 1)[0].lower()
        if local_part in _GENERIC_EMAIL_PREFIXES:
            confidence -= 0.08
        if email.lower().endswith(".edu"):
            confidence += 0.04
        if not _email_looks_relevant_to_job(email, job):
            confidence -= 0.2
        return max(0.0, min(0.95, confidence))

    def _score_instagram_candidate(self, instagram_url: str, document: SearchDocument, job: FieldJob, *, direct_url: bool) -> float:
        provider_base = {
            "provenance": 0.72,
            "chapter_website": 0.9,
            "search_result": 0.82,
            "search_page": 0.84,
        }.get(document.provider, 0.68)
        confidence = provider_base + (0.04 if direct_url else 0.0)
        confidence += 0.03 * _score_result_context(job, f"{instagram_url} {document.title or ''} {document.text[:200]}")
        if not _instagram_looks_relevant_to_job(instagram_url, job):
            confidence -= 0.22
        return max(0.0, min(0.95, confidence))

    def _search_documents(self, job: FieldJob, target: str) -> list[SearchDocument]:
        documents: list[SearchDocument] = []
        source_text = self._source_text(job)
        if source_text:
            documents.append(SearchDocument(text=source_text, provider="provenance", url=job.source_base_url))
        if job.website_url:
            website_document = self._fetch_search_document(job.website_url, provider="chapter_website")
            if website_document is not None:
                documents.append(website_document)

        seen_urls: set[str] = set()
        for query in self._build_search_queries(job, target):
            for result in self._run_search(query):
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
                if len(seen_urls) >= self._max_search_pages or _should_skip_search_page_fetch(result.url):
                    seen_urls.add(result.url)
                    continue
                fetched = self._fetch_search_document(result.url, provider="search_page", query=query)
                if fetched is not None:
                    documents.append(fetched)
                seen_urls.add(result.url)
        return documents

    def _build_search_queries(self, job: FieldJob, target: str) -> list[str]:
        fraternity = _display_name(job.fraternity_slug)
        chapter = job.chapter_name or _display_name(job.chapter_slug)
        university = job.university_name or str(job.payload.get("candidateSchoolName") or "")
        include_chapter = bool(chapter and not _is_generic_greek_letter_chapter_name(chapter))

        query_parts: list[list[str]] = []
        if target == "website":
            if include_chapter:
                query_parts.extend(
                    [
                        [fraternity, chapter, university, "chapter website"],
                        [fraternity, chapter, university, "official chapter site"],
                    ]
                )
            query_parts.extend(
                [
                    [fraternity, university, "chapter website"],
                    [fraternity, university, "official chapter site"],
                    [fraternity, university, "student organization", "site:.edu"],
                    [fraternity, university, "greek life", "site:.edu"],
                    [university, fraternity, "fraternity", "site:.edu"],
                    [f'"{fraternity}"' if fraternity else "", f'"{university}"' if university else "", "fraternity", "site:.edu"],
                ]
            )
        elif target == "email":
            if include_chapter:
                query_parts.extend(
                    [
                        [fraternity, chapter, university, "email"],
                        [fraternity, chapter, university, "contact email"],
                    ]
                )
            query_parts.extend(
                [
                    [fraternity, university, "email"],
                    [fraternity, university, "contact email"],
                    [fraternity, university, "contact", "site:.edu"],
                    [f'"{fraternity}"' if fraternity else "", f'"{university}"' if university else "", "email", "site:.edu"],
                ]
            )
        else:
            if include_chapter:
                query_parts.extend(
                    [
                        [fraternity, chapter, university, "instagram"],
                        [fraternity, chapter, university, "instagram profile"],
                    ]
                )
            query_parts.extend(
                [
                    [fraternity, university, "instagram"],
                    [fraternity, university, "instagram profile"],
                    [fraternity, university, "site:instagram.com"],
                    [f'"{fraternity}"' if fraternity else "", f'"{university}"' if university else "", "site:instagram.com"],
                ]
            )

        queries = [" ".join(part for part in parts if part).strip() for parts in query_parts]
        return list(dict.fromkeys(query for query in queries if query))

    def _run_search(self, query: str) -> list[SearchResult]:
        if self._search_client is None:
            return []
        try:
            results = self._search_client.search(query)
        except SearchUnavailableError as exc:
            log_event(self._logger, "search_unavailable", level=30, query=query, error=str(exc))
            return []
        except requests.RequestException as exc:
            log_event(self._logger, "search_request_failed", level=30, query=query, error=str(exc))
            return []
        log_event(self._logger, "search_query_executed", query=query, result_count=len(results))
        return results

    def _fetch_search_document(self, url: str, provider: str, query: str | None = None) -> SearchDocument | None:
        try:
            response = self._get_requester(url, timeout=10, allow_redirects=True)
        except requests.RequestException:
            return None

        status_code = getattr(response, "status_code", None)
        if status_code is not None and status_code >= 400:
            return None

        html = getattr(response, "text", "") or ""
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        links = [href.strip() for href in (node.get("href") for node in soup.select("a[href]")) if href and href.strip()]
        text = " ".join(soup.stripped_strings)
        title = soup.title.get_text(" ", strip=True) if soup.title else None
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



    def _score_website_candidate(self, website_url: str, document: SearchDocument, job: FieldJob) -> float:
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
        if document.provider == "search_page" and document_host and candidate_host and document_host != candidate_host:
            confidence += 0.05
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


def _significant_tokens(value: str | None) -> list[str]:
    tokens = [token for token in _normalized_match_text(value).split() if len(token) >= 4 and token not in _MATCH_STOPWORDS]
    if tokens:
        return tokens
    return [token for token in _normalized_match_text(value).split() if len(token) >= 3 and token not in {"the", "and", "of", "for"}]


def _fraternity_matches(job: FieldJob, text: str) -> bool:
    fraternity_phrase = _normalized_match_text(_display_name(job.fraternity_slug))
    if fraternity_phrase and fraternity_phrase in text:
        return True
    tokens = _significant_tokens(_display_name(job.fraternity_slug))
    if not tokens:
        return False
    return sum(1 for token in tokens if token in text) >= min(2, len(tokens))


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


def _email_looks_relevant_to_job(email: str, job: FieldJob) -> bool:
    lowered = email.lower()
    return _fraternity_matches(job, _normalized_match_text(lowered)) or any(token in lowered for token in _significant_tokens(job.university_name or str(job.payload.get("candidateSchoolName") or "")))


def _instagram_looks_relevant_to_job(instagram_url: str, job: FieldJob) -> bool:
    handle = _normalized_match_text(instagram_url.rsplit("/", 1)[-1])
    tokens = set(_significant_tokens(job.chapter_name) + _significant_tokens(job.university_name or str(job.payload.get("candidateSchoolName") or "")) + _significant_tokens(_display_name(job.fraternity_slug)))
    return any(token in handle for token in tokens if len(token) >= 3)


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

class RetryableJobError(Exception):
    pass



