from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.config import get_settings
from fratfinder_crawler.llm.client import LLMClient, LLMUnavailableError
from fratfinder_crawler.llm.extractor import ExtractionValidationError, LLMExtractionResult, extract_records
from fratfinder_crawler.models import CrawlMetrics, ExtractedChapter, PageAnalysis, SourceClassification, SourceRecord
from fratfinder_crawler.orchestration.graph import CrawlOrchestrator


UNKNOWN_PAGE_HTML = "<html><body><h1>About Us</h1><p>History and values.</p></body></html>"


@pytest.fixture(autouse=True)
def clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class FakeRepository:
    def __init__(self) -> None:
        self._run_id = 0
        self.persisted_chapters: list[tuple[str, object]] = []
        self.provenance_records: list[tuple[str, list[object]]] = []
        self.review_items: list[object] = []
        self.field_jobs: list[dict[str, object]] = []
        self.finished_runs: list[dict[str, object]] = []

    def start_crawl_run(self, source_id: str) -> int:
        self._run_id += 1
        return self._run_id

    def finish_crawl_run(self, run_id: int, status: str, metrics: CrawlMetrics, last_error: str | None = None, **kwargs) -> None:
        self.finished_runs.append({"run_id": run_id, "status": status, "metrics": metrics, "last_error": last_error, **kwargs})

    def upsert_chapter(self, source: SourceRecord, chapter) -> str:
        chapter_id = f"chapter-{len(self.persisted_chapters) + 1}"
        self.persisted_chapters.append((chapter_id, chapter))
        return chapter_id

    def insert_provenance(self, chapter_id: str, source_id: str, crawl_run_id: int, records: list[object]) -> None:
        self.provenance_records.append((chapter_id, records))

    def create_review_item(self, source_id: str, crawl_run_id: int, candidate, chapter_id: str | None = None) -> None:
        self.review_items.append(candidate)

    def create_field_jobs(
        self,
        chapter_id: str,
        crawl_run_id: int,
        chapter_slug: str,
        source_slug: str,
        missing_fields: list[str],
    ) -> int:
        self.field_jobs.append(
            {
                "chapter_id": chapter_id,
                "crawl_run_id": crawl_run_id,
                "chapter_slug": chapter_slug,
                "source_slug": source_slug,
                "missing_fields": list(missing_fields),
            }
        )
        return len(missing_fields)


class FakeHttpClient:
    def __init__(self, html: str) -> None:
        self._html = html

    def get(self, url: str) -> str:
        return self._html



def _source() -> SourceRecord:
    return SourceRecord(
        id="source-1",
        fraternity_id="frat-1",
        fraternity_slug="sigma-chi",
        source_slug="sigma-chi-main",
        source_type="html_directory",
        parser_key="directory_v1",
        base_url="https://example.org",
        list_path="/chapters",
        metadata={},
    )



def _page_analysis() -> PageAnalysis:
    return PageAnalysis(
        title="Chapter Directory",
        headings=["Find a Chapter"],
        table_count=0,
        repeated_block_count=0,
        link_count=1,
        has_json_ld=False,
        has_script_json=False,
        has_map_widget=False,
        has_pagination=False,
        probable_page_role="unknown",
        text_sample="Alpha Chapter at Example University in Columbus, OH. Contact alpha@example.edu.",
    )



def _enable_llm(monkeypatch: pytest.MonkeyPatch, max_calls: int = 3) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/fratfinder")
    monkeypatch.setenv("CRAWLER_LLM_ENABLED", "true")
    monkeypatch.setenv("CRAWLER_LLM_MODEL", "gpt-4o-mini")
    monkeypatch.setenv("CRAWLER_LLM_MAX_TOKENS", "2000")
    monkeypatch.setenv("CRAWLER_LLM_MAX_CALLS_PER_RUN", str(max_calls))
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    get_settings.cache_clear()



def _disable_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/fratfinder")
    monkeypatch.setenv("CRAWLER_LLM_ENABLED", "false")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    get_settings.cache_clear()



def _make_fake_openai(payload: dict, sink: dict[str, object]):
    class FakeOpenAI:
        def __init__(self, api_key: str):
            sink["api_key"] = api_key
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

        def _create(self, **kwargs):
            sink["kwargs"] = kwargs
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(content=json.dumps(payload))
                    )
                ]
            )

    return FakeOpenAI



def test_llm_client_raises_when_disabled_or_key_missing(monkeypatch: pytest.MonkeyPatch):
    _disable_llm(monkeypatch)

    with pytest.raises(LLMUnavailableError):
        LLMClient()

    monkeypatch.setenv("CRAWLER_LLM_ENABLED", "true")
    monkeypatch.setenv("OPENAI_API_KEY", "")
    get_settings.cache_clear()

    with pytest.raises(LLMUnavailableError):
        LLMClient()



def test_llm_extractor_returns_records_from_mocked_openai_response(monkeypatch: pytest.MonkeyPatch):
    _enable_llm(monkeypatch)
    sink: dict[str, object] = {}
    payload = {
        "records": [
            {
                "chapter_name": "Alpha Chapter",
                "school_name": "Example University",
                "city": "Columbus",
                "state": "OH",
                "address": None,
                "website_url": "https://chapters.example.org/alpha",
                "instagram_url": "https://instagram.com/alpha",
                "email": "alpha@example.edu",
                "source_confidence": 0.82,
            }
        ],
        "page_level_confidence": 0.78,
        "extraction_notes": "Single chapter found in visible text.",
    }
    client = LLMClient(client_factory=_make_fake_openai(payload, sink))

    records = extract_records(_page_analysis(), "https://example.org/chapters", llm_client=client)

    assert len(records) == 1
    assert records[0].name == "Alpha Chapter"
    assert records[0].university_name == "Example University"
    assert records[0].website_url == "https://chapters.example.org/alpha"
    assert records[0].instagram_url == "https://instagram.com/alpha"
    assert records[0].contact_email == "alpha@example.edu"
    assert records[0].source_confidence == 0.82
    assert sink["api_key"] == "test-key"
    assert sink["kwargs"]["response_format"]["type"] == "json_schema"



def test_llm_extractor_raises_validation_error_for_malformed_response(monkeypatch: pytest.MonkeyPatch):
    _enable_llm(monkeypatch)
    payload = {
        "records": [
            {
                "school_name": "Example University",
                "city": "Columbus",
                "state": "OH",
                "address": None,
                "website_url": None,
                "instagram_url": None,
                "email": None,
                "source_confidence": 0.81,
            }
        ],
        "page_level_confidence": 0.74,
        "extraction_notes": "Malformed for test.",
    }
    client = LLMClient(client_factory=_make_fake_openai(payload, {}))

    with pytest.raises(ExtractionValidationError):
        extract_records(_page_analysis(), "https://example.org/chapters", llm_client=client)



def test_llm_call_budget_is_enforced_across_classification_and_extraction(monkeypatch: pytest.MonkeyPatch):
    _enable_llm(monkeypatch, max_calls=1)
    repository = FakeRepository()
    orchestrator = CrawlOrchestrator(repository, FakeHttpClient(UNKNOWN_PAGE_HTML), AdapterRegistry())
    classifier_calls = {"count": 0}
    extractor_calls = {"count": 0}

    def fake_classify(page_analysis: PageAnalysis) -> SourceClassification:
        classifier_calls["count"] += 1
        return SourceClassification(
            page_type="unsupported_or_unclear",
            confidence=0.95,
            recommended_strategy="llm",
            needs_follow_links=False,
            possible_data_locations=[],
            classified_by="llm",
        )

    def fake_extract(page_analysis: PageAnalysis, source_url: str) -> LLMExtractionResult:
        extractor_calls["count"] += 1
        raise AssertionError("Extractor should not be called when the LLM budget is exhausted")

    monkeypatch.setattr("fratfinder_crawler.orchestration.graph.classify_source_with_llm", fake_classify)
    monkeypatch.setattr("fratfinder_crawler.orchestration.graph.extract_records_with_metadata", fake_extract)

    final_state = orchestrator._graph.invoke(
        {
            "source": _source(),
            "run_id": 1,
            "review_items": [],
            "metrics": CrawlMetrics(),
            "final_status": "succeeded",
            "strategy_attempts": 0,
            "llm_calls_used": 0,
        }
    )

    assert classifier_calls["count"] == 1
    assert extractor_calls["count"] == 0
    assert final_state["llm_calls_used"] == 1
    assert final_state["metrics"].records_upserted == 0
    assert final_state["metrics"].review_items_created == 1
    assert repository.review_items[0].item_type == "llm_budget_exhausted"
    assert repository.finished_runs[-1]["status"] == "partial"




def test_graph_handles_invalid_llm_extraction_without_crashing(monkeypatch: pytest.MonkeyPatch):
    _enable_llm(monkeypatch, max_calls=2)
    repository = FakeRepository()
    orchestrator = CrawlOrchestrator(repository, FakeHttpClient(UNKNOWN_PAGE_HTML), AdapterRegistry())

    def fake_classify(page_analysis: PageAnalysis) -> SourceClassification:
        return SourceClassification(
            page_type="unsupported_or_unclear",
            confidence=0.92,
            recommended_strategy="llm",
            needs_follow_links=False,
            possible_data_locations=[],
            classified_by="llm",
        )

    def fake_extract(page_analysis: PageAnalysis, source_url: str) -> LLMExtractionResult:
        raise ExtractionValidationError("schema mismatch")

    monkeypatch.setattr("fratfinder_crawler.orchestration.graph.classify_source_with_llm", fake_classify)
    monkeypatch.setattr("fratfinder_crawler.orchestration.graph.extract_records_with_metadata", fake_extract)

    metrics = orchestrator.run_for_source(_source())

    assert metrics.records_upserted == 0
    assert metrics.review_items_created == 1
    assert repository.review_items[0].item_type == "llm_extraction_invalid"
    assert repository.finished_runs[-1]["status"] == "partial"
def test_unknown_page_with_llm_enabled_writes_low_confidence_field_states(monkeypatch: pytest.MonkeyPatch):
    _enable_llm(monkeypatch, max_calls=2)
    repository = FakeRepository()
    orchestrator = CrawlOrchestrator(repository, FakeHttpClient(UNKNOWN_PAGE_HTML), AdapterRegistry())

    def fake_classify(page_analysis: PageAnalysis) -> SourceClassification:
        return SourceClassification(
            page_type="unsupported_or_unclear",
            confidence=0.92,
            recommended_strategy="llm",
            needs_follow_links=False,
            possible_data_locations=[],
            classified_by="llm",
        )

    def fake_extract(page_analysis: PageAnalysis, source_url: str) -> LLMExtractionResult:
        return LLMExtractionResult(
            records=[
                ExtractedChapter(
                    name="Alpha Chapter",
                    university_name="Example University",
                    city="Columbus",
                    state="OH",
                    website_url="https://chapters.example.org/alpha",
                    source_url=source_url,
                    source_confidence=0.7,
                )
            ],
            page_level_confidence=0.8,
            extraction_notes="One likely chapter found in visible text.",
        )

    monkeypatch.setattr("fratfinder_crawler.orchestration.graph.classify_source_with_llm", fake_classify)
    monkeypatch.setattr("fratfinder_crawler.orchestration.graph.extract_records_with_metadata", fake_extract)

    metrics = orchestrator.run_for_source(_source())

    assert metrics.records_upserted == 1
    assert metrics.review_items_created == 0
    assert len(repository.persisted_chapters) == 1
    persisted_chapter = repository.persisted_chapters[0][1]
    assert persisted_chapter.field_states["name"] == "low_confidence"
    assert persisted_chapter.field_states["university_name"] == "low_confidence"
    assert persisted_chapter.field_states["website_url"] == "low_confidence"
    assert persisted_chapter.field_states["instagram_url"] == "missing"
    assert persisted_chapter.field_states["contact_email"] == "missing"
    assert repository.finished_runs[-1]["status"] == "succeeded"






