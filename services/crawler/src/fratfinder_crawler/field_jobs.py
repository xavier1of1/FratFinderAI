from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

import requests

from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import (
    FieldJob,
    FIELD_JOB_FIND_EMAIL,
    FIELD_JOB_FIND_INSTAGRAM,
    FIELD_JOB_FIND_WEBSITE,
    FIELD_JOB_TO_STATE_KEY,
    FIELD_JOB_VERIFY_SCHOOL,
    FIELD_JOB_VERIFY_WEBSITE,
    ReviewItemCandidate,
)

if TYPE_CHECKING:
    from fratfinder_crawler.db.repository import CrawlerRepository

_EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b")
_INSTAGRAM_RE = re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.-]+", re.IGNORECASE)
_URL_RE = re.compile(r'https?://[^\s\]\[\)\("]+', re.IGNORECASE)


@dataclass(slots=True)
class FieldJobResult:
    chapter_updates: dict[str, str]
    completed_payload: dict[str, str]
    field_state_updates: dict[str, str] = field(default_factory=dict)
    review_item: ReviewItemCandidate | None = None


class FieldJobEngine:
    def __init__(
        self,
        repository: CrawlerRepository,
        logger,
        worker_id: str,
        base_backoff_seconds: int = 30,
        source_slug: str | None = None,
        head_requester: Callable[..., object] | None = None,
    ):
        self._repository = repository
        self._logger = logger
        self._worker_id = worker_id
        self._base_backoff_seconds = max(1, base_backoff_seconds)
        self._source_slug = source_slug
        self._head_requester = head_requester or requests.head

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
            email = _first_match(_EMAIL_RE, self._source_text(job))
            if not email:
                raise RetryableJobError("No candidate email found in provenance snippets")
            return FieldJobResult(
                chapter_updates={"contact_email": email},
                completed_payload={"status": "updated", "contact_email": email},
                field_state_updates={"contact_email": "found"},
            )

        if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
            if job.instagram_url:
                return self._already_populated_result(job.field_name, job.instagram_url)
            instagram = _first_match(_INSTAGRAM_RE, self._source_text(job))
            if not instagram:
                raise RetryableJobError("No candidate instagram URL found in provenance snippets")
            return FieldJobResult(
                chapter_updates={"instagram_url": instagram},
                completed_payload={"status": "updated", "instagram_url": instagram},
                field_state_updates={"instagram_url": "found"},
            )

        if job.field_name == FIELD_JOB_FIND_WEBSITE:
            if job.website_url:
                return self._already_populated_result(job.field_name, job.website_url)
            website = _first_non_instagram_url(self._source_text(job))
            if not website and job.source_base_url:
                website = job.source_base_url
            if not website:
                raise RetryableJobError("No candidate website URL available")
            return FieldJobResult(
                chapter_updates={"website_url": website},
                completed_payload={"status": "updated", "website_url": website},
                field_state_updates={"website_url": "found"},
            )

        if job.field_name == FIELD_JOB_VERIFY_WEBSITE:
            return self._verify_website(job)

        if job.field_name == FIELD_JOB_VERIFY_SCHOOL:
            return self._verify_school_match(job)

        raise RetryableJobError(f"Unsupported field job type: {job.field_name}")

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



def _first_match(pattern: re.Pattern[str], value: str) -> str | None:
    match = pattern.search(value)
    return match.group(0) if match else None



def _first_non_instagram_url(value: str) -> str | None:
    for match in _URL_RE.findall(value):
        if "instagram.com" in match.lower():
            continue
        return match
    return None



def _slugify(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"^-+|-+$", "", text)


class RetryableJobError(Exception):
    pass
