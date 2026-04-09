from pathlib import Path

from fratfinder_crawler.analysis import analyze_page, classify_source, detect_embedded_data, select_extraction_plan
from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.models import CrawlMetrics, EmbeddedDataResult, PageAnalysis, SourceClassification, SourceRecord
from fratfinder_crawler.orchestration.graph import CrawlOrchestrator


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


UNKNOWN_PAGE_HTML = "<html><body><h1>About Us</h1><p>History and values.</p></body></html>"


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


def test_static_directory_fixture_classifies_with_high_confidence():
    html = Path("services/crawler/fixtures/static_directory.html").read_text(encoding="utf-8")

    analysis = analyze_page(html)
    classification = classify_source(analysis, llm_enabled=False)

    assert classification.page_type == "static_directory"
    assert classification.confidence >= 0.80
    assert classification.recommended_strategy in ["repeated_block", "table"]



def test_single_explicit_chapter_card_still_classifies_as_directory():
    html = """
    <html><body>
      <ul>
        <li class=\"chapter-item\" data-chapter-card>
          <h3 class=\"chapter-name\">Alpha Test</h3>
          <div class=\"university\">Demo University</div>
          <div class=\"location\">Austin, TX</div>
        </li>
      </ul>
    </body></html>
    """

    analysis = analyze_page(html)
    classification = classify_source(analysis, llm_enabled=False)

    assert analysis.probable_page_role == "directory"
    assert classification.page_type == "static_directory"
    assert classification.recommended_strategy == "repeated_block"


def test_div_chapter_items_and_anchor_cards_classify_as_directory():
    html = """
    <html><body>
      <section class="chapters-grid">
        <div class="chapter-item">
          <h2>Alpha</h2>
          <h3>Norwich University</h3>
        </div>
        <a class="chapter-link" href="/eta">
          <h2>Eta</h2>
          <h3>University of Rhode Island</h3>
        </a>
      </section>
    </body></html>
    """

    analysis = analyze_page(html)
    classification = classify_source(analysis, llm_enabled=False)

    assert analysis.repeated_block_count >= 2
    assert analysis.probable_page_role == "directory"
    assert classification.page_type == "static_directory"
    assert classification.recommended_strategy == "repeated_block"


def test_crawl_orchestrator_uses_directory_layout_profile_for_linked_directory():
    html = """
    <html><body>
      <h1>Chapters</h1>
      <ul>
        <li><a href="/alpha">State University - Alpha Chapter</a></li>
        <li><a href="/beta">Central College - Beta Chapter</a></li>
        <li><a href="/gamma">Northern Tech - Gamma Chapter</a></li>
        <li><a href="/delta">Western State - Delta Chapter</a></li>
        <li><a href="/epsilon">Coastal University - Epsilon Chapter</a></li>
        <li><a href="/zeta">Valley University - Zeta Chapter</a></li>
      </ul>
    </body></html>
    """

    orchestrator = CrawlOrchestrator(FakeRepository(), FakeHttpClient(html), AdapterRegistry())
    state = {"source": _source(), "run_id": 1, "html": html, "page_analysis": analyze_page(html)}

    state.update(orchestrator._profile_directory_layout(state))
    updates = orchestrator._classify_source_type(state)

    assert updates["classification"].page_type == "static_directory"
    assert updates["classification"].recommended_strategy == "repeated_block"


def test_embedded_data_detector_flags_json_ld_chapter_data():
    html = """
    <html>
      <body>
        <script type="application/ld+json">
          [
            {"@type": "EducationalOrganization", "name": "Alpha Chapter", "url": "https://example.org/alpha"},
            {"@type": "EducationalOrganization", "name": "Beta Chapter", "url": "https://example.org/beta"}
          ]
        </script>
      </body>
    </html>
    """

    result = detect_embedded_data(html, "https://example.org/chapters")

    assert result.found is True
    assert result.data_type == "json_ld"
    assert result.raw_data is not None
    assert len(result.raw_data) == 2


def test_embedded_data_detector_extracts_google_maps_kml_api_hint():
    html = """
    <html>
      <body>
        <iframe src="https://www.google.com/maps/d/u/0/embed?mid=1497z-lFQzqOBrDnwB3z0r_qiqNU"></iframe>
      </body>
    </html>
    """

    result = detect_embedded_data(html, "https://example.org/chapters")

    assert result.found is True
    assert result.data_type == "api_hint"
    assert result.api_url is not None
    assert "google.com/maps/d/kml" in result.api_url
    assert "mid=1497z-lFQzqOBrDnwB3z0r_qiqNU" in result.api_url



def test_strategy_selector_prefers_known_directory_over_script_json_false_positive():
    page_analysis = PageAnalysis(
        title="Sigma Chi Undergraduate Groups",
        headings=["Undergraduate Groups"],
        table_count=2,
        repeated_block_count=50,
        link_count=120,
        has_json_ld=False,
        has_script_json=True,
        has_map_widget=False,
        has_pagination=False,
        probable_page_role="directory",
        text_sample="Chapter directory",
    )
    classification = SourceClassification(
        page_type="static_directory",
        confidence=0.9,
        recommended_strategy="table",
        needs_follow_links=False,
        possible_data_locations=["table"],
        classified_by="heuristic",
    )
    embedded_data = EmbeddedDataResult(found=True, data_type="script_json", raw_data=[], api_url=None)

    plan = select_extraction_plan(page_analysis, classification, embedded_data, llm_enabled=False)

    assert plan.primary_strategy == "table"
    assert "script_json" in plan.fallback_strategies


def test_strategy_selector_never_returns_llm_when_disabled():
    page_analysis = PageAnalysis(
        title="Unknown",
        headings=["Welcome"],
        table_count=0,
        repeated_block_count=0,
        link_count=1,
        has_json_ld=False,
        has_script_json=False,
        has_map_widget=False,
        has_pagination=False,
        probable_page_role="unknown",
        text_sample="Welcome",
    )
    classification = SourceClassification(
        page_type="unsupported_or_unclear",
        confidence=0.25,
        recommended_strategy="llm",
        needs_follow_links=False,
        possible_data_locations=[],
        classified_by="heuristic",
    )
    embedded_data = EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None)

    plan = select_extraction_plan(page_analysis, classification, embedded_data, llm_enabled=False)

    assert plan.primary_strategy == "review"
    assert plan.primary_strategy != "llm"
    assert plan.llm_allowed is False



def test_unknown_page_routes_to_review_without_crashing():
    page_analysis = analyze_page(UNKNOWN_PAGE_HTML)
    classification = classify_source(page_analysis, llm_enabled=False)
    plan = select_extraction_plan(
        page_analysis,
        classification,
        EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        llm_enabled=False,
    )

    assert classification.page_type == "unsupported_or_unclear"
    assert plan.primary_strategy == "review"

    repository = FakeRepository()
    orchestrator = CrawlOrchestrator(repository, FakeHttpClient(UNKNOWN_PAGE_HTML), AdapterRegistry())

    metrics = orchestrator.run_for_source(_source())

    assert metrics.records_upserted == 0
    assert metrics.review_items_created == 1
    assert repository.review_items[0].item_type == "unsupported_or_unclear_source"
    assert repository.finished_runs[-1]["status"] == "partial"



def test_full_graph_executes_end_to_end_against_existing_sample_fixture():
    html = Path("services/crawler/fixtures/sample_directory.html").read_text(encoding="utf-8")
    repository = FakeRepository()
    orchestrator = CrawlOrchestrator(repository, FakeHttpClient(html), AdapterRegistry())

    final_state = orchestrator._graph.invoke(
        {
            "source": _source(),
            "run_id": 1,
            "review_items": [],
            "metrics": CrawlMetrics(),
            "final_status": "succeeded",
            "strategy_attempts": 0,
        }
    )

    assert final_state["page_analysis"].probable_page_role == "directory"
    assert final_state["classification"].page_type == "static_directory"
    assert final_state["embedded_data"].found is False
    assert final_state["extraction_plan"].primary_strategy in {"repeated_block", "table"}
    assert final_state["strategy_attempts"] == 1
    assert final_state["metrics"].records_upserted == 2
    assert final_state["metrics"].review_items_created == 0
    assert len(repository.persisted_chapters) == 2
    assert repository.finished_runs[-1]["extraction_metadata"]["strategy_used"] in {"repeated_block", "table"}
    assert repository.finished_runs[-1]["classification"]["page_type"] == "static_directory"






def test_strategy_selector_respects_source_metadata_override():
    page_analysis = PageAnalysis(
        title="Chapters",
        headings=["Chapters"],
        table_count=0,
        repeated_block_count=0,
        link_count=3,
        has_json_ld=False,
        has_script_json=False,
        has_map_widget=False,
        has_pagination=False,
        probable_page_role="unknown",
        text_sample="chapter archive",
    )
    classification = SourceClassification(
        page_type="unsupported_or_unclear",
        confidence=0.2,
        recommended_strategy="review",
        needs_follow_links=False,
        possible_data_locations=[],
        classified_by="heuristic",
    )
    embedded_data = EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None)

    plan = select_extraction_plan(
        page_analysis,
        classification,
        embedded_data,
        llm_enabled=False,
        source_metadata={
            "extractionHints": {
                "primaryStrategy": "repeated_block",
                "fallbackStrategies": ["table", "review"],
            }
        },
    )

    assert plan.primary_strategy == "repeated_block"
    assert plan.fallback_strategies == ["table", "review"]
    assert plan.source_hint_applied == "primaryStrategy"

