from __future__ import annotations

from fratfinder_crawler.config import Settings
from fratfinder_crawler.field_jobs import CandidateMatch
from fratfinder_crawler.social import bulk_backfill_instagram


class _FakeRepository:
    def __init__(self) -> None:
        self.applied: list[dict[str, object]] = []
        self.completed: list[dict[str, object]] = []

    def apply_instagram_resolution(self, **kwargs) -> bool:
        self.applied.append(kwargs)
        return True

    def complete_pending_field_jobs_for_chapter(self, **kwargs) -> int:
        self.completed.append(kwargs)
        return 1


class _AlwaysMatchingProbeEngine:
    def __init__(self, *_args, **_kwargs) -> None:
        self._search_document_cache: dict[str, object] = {}

    def _probe_instagram_handle_candidates(self, job):
        self._search_document_cache[job.chapter_id] = object()
        return [
            CandidateMatch(
                value="https://www.instagram.com/tculambdachi/",
                confidence=0.91,
                source_url="https://www.instagram.com/tculambdachi/",
                source_snippet="Official Lambda Chi Alpha chapter at TCU",
                field_name="instagram_url",
                source_provider="instagram_probe",
                query="instagram_probe:tculambdachi",
            )
        ]

    def _found_threshold(self, _job, _target_field: str, _match: CandidateMatch) -> float:
        return 0.84


def _row(chapter_id: str = "chapter-1") -> dict[str, object]:
    return {
        "chapter_id": chapter_id,
        "chapter_slug": "chapter-one",
        "chapter_name": "Iota-Pi",
        "university_name": "Texas Christian University",
        "city": "Fort Worth",
        "state": "TX",
        "website_url": "https://www.tcufiji.com/",
        "chapter_status": "active",
        "field_states": {"website_url": "found"},
        "fraternity_slug": "lambda-chi-alpha",
        "fraternity_name": "Lambda Chi Alpha",
        "source_slug": "lambda-chi-alpha-main",
        "source_base_url": "https://www.lambdachi.org/",
        "crawl_run_id": 101,
        "request_id": None,
    }


def _duplicate_row(chapter_id: str = "chapter-2") -> dict[str, object]:
    return {
        **_row(chapter_id),
        "donor_chapter_slug": "chapter-one-donor",
        "donor_instagram_url": "https://www.instagram.com/tculambdachi/",
        "donor_source_url": "https://www.instagram.com/tculambdachi/",
    }


def test_probe_backfill_applies_and_closes_pending_jobs(monkeypatch):
    repo = _FakeRepository()
    stats = bulk_backfill_instagram.BackfillStats(mode="probe")
    settings = Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder")

    monkeypatch.setattr(bulk_backfill_instagram, "FieldJobEngine", _AlwaysMatchingProbeEngine)

    bulk_backfill_instagram._apply_probe_backfill(
        repo,
        [_row()],
        existing_assignments={},
        close_pending_jobs=True,
        dry_run=False,
        settings=settings,
        stats=stats,
    )

    assert stats.chapters_examined == 1
    assert stats.chapters_with_candidates == 1
    assert stats.accepted_candidates == 1
    assert stats.applied == 1
    assert stats.pending_jobs_completed == 1
    assert repo.applied[0]["instagram_url"] == "https://www.instagram.com/tculambdachi/"
    assert repo.applied[0]["reason_code"] == "accepted_bulk_probe_instagram"
    assert repo.completed[0]["reason_code"] == "resolved_by_bulk_instagram_probe"


def test_probe_backfill_skips_duplicate_assignment(monkeypatch):
    repo = _FakeRepository()
    stats = bulk_backfill_instagram.BackfillStats(mode="probe")
    settings = Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder")

    monkeypatch.setattr(bulk_backfill_instagram, "FieldJobEngine", _AlwaysMatchingProbeEngine)

    bulk_backfill_instagram._apply_probe_backfill(
        repo,
        [_row()],
        existing_assignments={"https://www.instagram.com/tculambdachi/": ["other-chapter"]},
        close_pending_jobs=True,
        dry_run=False,
        settings=settings,
        stats=stats,
    )

    assert stats.chapters_examined == 1
    assert stats.chapters_with_candidates == 1
    assert stats.accepted_candidates == 0
    assert stats.applied == 0
    assert repo.applied == []
    assert repo.completed == []


def test_duplicate_backfill_applies_same_school_donor_and_closes_pending_jobs():
    repo = _FakeRepository()
    stats = bulk_backfill_instagram.BackfillStats(mode="duplicate")

    bulk_backfill_instagram._apply_duplicate_backfill(
        repo,
        [_duplicate_row()],
        existing_assignments={},
        close_pending_jobs=True,
        dry_run=False,
        stats=stats,
    )

    assert stats.chapters_examined == 1
    assert stats.chapters_with_candidates == 1
    assert stats.accepted_candidates == 1
    assert stats.applied == 1
    assert stats.pending_jobs_completed == 1
    assert repo.applied[0]["reason_code"] == "accepted_same_school_duplicate_instagram"
    assert repo.applied[0]["instagram_url"] == "https://www.instagram.com/tculambdachi/"
    assert repo.completed[0]["reason_code"] == "resolved_by_bulk_duplicate_instagram"
