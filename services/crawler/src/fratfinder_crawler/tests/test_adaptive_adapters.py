from pathlib import Path

from fratfinder_crawler.adapters.locator_api import LocatorApiAdapter
from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.adapters.script_json import ScriptJsonAdapter
from fratfinder_crawler.adapters.directory_v1 import DirectoryV1Adapter
from fratfinder_crawler.models import CrawlMetrics, SourceRecord
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


class RoutingHttpClient:
    def __init__(self, responses: dict[str, str]) -> None:
        self._responses = responses

    def get(self, url: str) -> str:
        if url not in self._responses:
            raise AssertionError(f"Unexpected URL requested: {url}")
        return self._responses[url]


class ExplodingHttpClient:
    def get(self, url: str) -> str:
        raise RuntimeError("boom")



def _source(source_type: str = "html_directory") -> SourceRecord:
    return SourceRecord(
        id="source-1",
        fraternity_id="frat-1",
        fraternity_slug="sigma-chi",
        source_slug="sigma-chi-main",
        source_type=source_type,
        parser_key="directory_v1",
        base_url="https://example.org",
        list_path="/chapters",
        metadata={},
    )



def test_script_json_adapter_extracts_window_chapters_fixture():
    fixture = Path("services/crawler/fixtures/sample_script_json.html").read_text(encoding="utf-8")

    records = ScriptJsonAdapter().parse(fixture, "https://example.org/chapters")

    assert len(records) == 2
    assert records[0].name == "Alpha Gamma"
    assert records[0].university_name == "Ohio State University"
    assert records[0].city == "Columbus"
    assert records[0].state == "OH"
    assert records[0].website_url == "https://chapters.example.org/alpha-gamma"
    assert records[0].external_id == "alpha-gamma"
    assert records[0].source_confidence >= 0.8



def test_script_json_adapter_extracts_json_ld_fixture():
    fixture = Path("services/crawler/fixtures/sample_json_ld_educational_organizations.html").read_text(encoding="utf-8")

    records = ScriptJsonAdapter().parse(fixture, "https://example.org/chapters")

    assert len(records) == 2
    assert records[0].name == "Gamma Delta"
    assert records[0].university_name == "University of Michigan"
    assert records[0].city == "Ann Arbor"
    assert records[0].state == "MI"
    assert records[0].website_url == "https://chapters.example.org/gamma-delta"
    assert records[0].external_id == "https://chapters.example.org/gamma-delta"
    assert records[0].source_confidence >= 0.85


def test_script_json_adapter_extracts_lambdachi_map_payload():
    html = """
    <script>
      window.chaptersMapData = [{
        "id": 18884,
        "title": "Epsilon-Psi (South Carolina)",
        "permalink": "https://www.lambdachi.org/chapters/epsilon-psi-south-carolina/",
        "country": "United States",
        "city": "Columbia",
        "state": "South Carolina",
        "street": "1400 Greene Street",
        "universitycollege": "University of South Carolina",
        "foundation_date": "19450512",
        "website": null,
        "instagram": "lambdachisc",
        "latitude": "33.99648105",
        "longitude": "-81.0269585784"
      }];
    </script>
    """

    records = ScriptJsonAdapter().parse(html, "https://www.lambdachi.org/chapters/")
    stubs = ScriptJsonAdapter().parse_stubs(html, "https://www.lambdachi.org/chapters/")

    assert len(records) == 1
    assert records[0].name == "Epsilon-Psi (South Carolina)"
    assert records[0].university_name == "University of South Carolina"
    assert records[0].city == "Columbia"
    assert records[0].state == "South Carolina"
    assert records[0].instagram_url == "https://www.instagram.com/lambdachisc"
    assert records[0].source_url == "https://www.lambdachi.org/chapters/epsilon-psi-south-carolina/"
    assert records[0].source_confidence >= 0.85
    assert len(stubs) == 1
    assert stubs[0].detail_url == "https://www.lambdachi.org/chapters/epsilon-psi-south-carolina/"



def test_locator_api_adapter_extracts_from_mocked_http_response():
    http_client = RoutingHttpClient(
        {
            "https://example.org/api/chapters": """
            [
              {
                \"chapterId\": \"epsilon-zeta\",
                \"chapterName\": \"Epsilon Zeta\",
                \"schoolName\": \"University of Georgia\",
                \"city\": \"Athens\",
                \"state\": \"GA\",
                \"websiteUrl\": \"https://chapters.example.org/epsilon-zeta\"
              }
            ]
            """
        }
    )

    records = LocatorApiAdapter().parse(
        "<html><body><div class='storepoint-map'></div></body></html>",
        "https://example.org/chapters",
        api_url="https://example.org/api/chapters",
        http_client=http_client,
    )

    assert len(records) == 1
    assert records[0].name == "Epsilon Zeta"
    assert records[0].university_name == "University of Georgia"
    assert records[0].city == "Athens"
    assert records[0].state == "GA"
    assert records[0].website_url == "https://chapters.example.org/epsilon-zeta"



def test_script_json_adapter_unknown_shape_returns_empty_list():
    fixture = """
    <html>
      <body>
        <script>
          window.chapters = [{"foo": "bar", "count": 2}];
        </script>
      </body>
    </html>
    """

    records = ScriptJsonAdapter().parse(fixture, "https://example.org/chapters")

    assert records == []



def test_locator_api_adapter_unknown_shape_returns_empty_list():
    http_client = RoutingHttpClient(
        {
            "https://example.org/api/chapters": "{\"items\": [{\"foo\": \"bar\"}]}"
        }
    )

    records = LocatorApiAdapter().parse(
        "<html></html>",
        "https://example.org/chapters",
        api_url="https://example.org/api/chapters",
        http_client=http_client,
    )

    assert records == []


def test_locator_api_adapter_extracts_from_google_maps_kml_payload():
    kml_payload = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
      <Document>
        <Placemark>
          <name>Case Western Reserve University</name>
          <description><![CDATA[
            Alias: Xi Deuteron Chapter<br>
            Preferred City_ State: Cleveland, OH<br>
            Website: http://cwrufiji.com/<br>
            Instagram: cwrufiji<br>
          ]]></description>
        </Placemark>
      </Document>
    </kml>
    """
    http_client = RoutingHttpClient({"https://www.google.com/maps/d/kml?mid=test&forcekml=1": kml_payload})

    records = LocatorApiAdapter().parse(
        "<html><body><iframe></iframe></body></html>",
        "https://example.org/chapters",
        api_url="https://www.google.com/maps/d/kml?mid=test&forcekml=1",
        http_client=http_client,
    )

    assert len(records) == 1
    assert records[0].name == "Xi Deuteron Chapter"
    assert records[0].university_name == "Case Western Reserve University"
    assert records[0].city == "Cleveland"
    assert records[0].state == "OH"
    assert records[0].website_url == "http://cwrufiji.com/"
    assert records[0].instagram_url == "https://www.instagram.com/cwrufiji/"


def test_locator_api_adapter_splits_combined_kml_name_when_alias_missing():
    kml_payload = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
      <Document>
        <Placemark>
          <name>Vanderbilt University - Gamma</name>
          <description><![CDATA[
            Preferred City_ State: Nashville, TN<br>
            Website: https://example.org/gamma<br>
          ]]></description>
        </Placemark>
      </Document>
    </kml>
    """
    http_client = RoutingHttpClient({"https://www.google.com/maps/d/kml?mid=test&forcekml=1": kml_payload})

    records = LocatorApiAdapter().parse(
        "<html><body><iframe></iframe></body></html>",
        "https://example.org/chapters",
        api_url="https://www.google.com/maps/d/kml?mid=test&forcekml=1",
        http_client=http_client,
    )

    assert len(records) == 1
    assert records[0].name == "Gamma"
    assert records[0].university_name == "Vanderbilt University"
    assert records[0].city == "Nashville"
    assert records[0].state == "TN"
    assert records[0].website_url == "https://example.org/gamma"



def test_graph_creates_review_item_when_script_json_strategy_extracts_no_records():
    html = """
    <html>
      <body>
        <h1>Chapter Directory</h1>
        <script>
          window.chapters = [{"foo": "bar"}];
        </script>
      </body>
    </html>
    """
    repository = FakeRepository()
    orchestrator = CrawlOrchestrator(repository, RoutingHttpClient({"https://example.org/chapters": html}), AdapterRegistry())

    metrics = orchestrator.run_for_source(_source())

    assert metrics.records_upserted == 0
    assert metrics.review_items_created == 1
    assert repository.review_items[0].item_type == "empty_extraction"
    assert repository.review_items[0].payload["strategy"] == "script_json"
    assert repository.finished_runs[-1]["status"] == "partial"



def test_graph_creates_review_item_when_locator_api_strategy_extracts_no_records():
    html = """
    <html>
      <body>
        <div class="storepoint-map"></div>
        <script>
          fetch('/api/chapters');
        </script>
      </body>
    </html>
    """
    repository = FakeRepository()
    http_client = RoutingHttpClient(
        {
            "https://example.org/chapters": html,
            "https://example.org/api/chapters": "{\"items\": [{\"foo\": \"bar\"}]}",
        }
    )
    orchestrator = CrawlOrchestrator(repository, http_client, AdapterRegistry())

    metrics = orchestrator.run_for_source(_source(source_type="locator_api"))

    assert metrics.records_upserted == 0
    assert metrics.review_items_created == 1
    assert repository.review_items[0].item_type == "empty_extraction"
    assert repository.review_items[0].payload["strategy"] == "locator_api"
    assert repository.finished_runs[-1]["status"] == "partial"



def test_locator_api_adapter_returns_empty_list_when_request_fails():
    records = LocatorApiAdapter().parse(
        "<html></html>",
        "https://example.org/chapters",
        api_url="https://example.org/api/chapters",
        http_client=ExplodingHttpClient(),
    )

    assert records == []


def test_graph_promotes_followed_directory_page_records_over_parent_stub():
    root_html = """
    <html>
      <body>
        <script>
          var uscanada_config = {
            'uscanada_1':{
              'hover': '<p>MISSISSIPPI</p>',
              'url':'https://example.org/chapters/mississippi/',
              'enbl':true
            }
          };
        </script>
      </body>
    </html>
    """
    mississippi_html = """
    <article>
      <section>
        <h2>MISSISSIPPI STATE CHAPTER</h2>
        <h2>PO Box GK Mississippi State Mississippi State, MS 39762</h2>
        <div class="elementor-widget-text-editor">
          <div class="elementor-widget-container">
            Website: <a href="http://msstatedeltachi.com/">Delta Chi Mississippi State</a>
            Instagram: <a href="https://www.instagram.com/msstatedeltachi">@msstatedeltachi</a>
          </div>
        </div>
      </section>
    </article>
    """
    repository = FakeRepository()
    http_client = RoutingHttpClient(
        {
            "https://example.org/chapters": root_html,
            "https://example.org/chapters/mississippi/": mississippi_html,
        }
    )
    orchestrator = CrawlOrchestrator(repository, http_client, AdapterRegistry())

    metrics = orchestrator.run_for_source(_source())

    assert metrics.records_upserted == 1
    assert len(repository.persisted_chapters) == 1
    persisted = repository.persisted_chapters[0][1]
    assert persisted.name == "Mississippi State Chapter"
    assert persisted.university_name == "Mississippi State"
    assert persisted.website_url == "http://msstatedeltachi.com/"


def test_directory_adapter_emits_stub_contract():
    html = """
    <li class="chapter-item" data-chapter-card>
      <h3 class="chapter-name">Gamma Chapter</h3>
      <div class="university">State University</div>
      <a href="/chapters/gamma">Go To Site</a>
    </li>
    """
    stubs = DirectoryV1Adapter().parse_stubs(html, "https://example.org/chapters")

    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Gamma Chapter"
    assert stubs[0].university_name == "State University"
    assert stubs[0].outbound_chapter_url_candidate == "https://example.org/chapters/gamma"
    assert stubs[0].provenance.startswith("directory_v1:")


def test_script_json_adapter_emits_stub_contract():
    html = """
    <script>
      window.chapters = [{"chapterName": "Epsilon Chapter", "schoolName": "Example College", "websiteUrl": "https://example.edu/epsilon"}];
    </script>
    """
    stubs = ScriptJsonAdapter().parse_stubs(html, "https://example.org/chapters")

    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Epsilon Chapter"
    assert stubs[0].university_name == "Example College"
    assert stubs[0].outbound_chapter_url_candidate == "https://example.edu/epsilon"


def test_locator_api_adapter_emits_stub_contract():
    http_client = RoutingHttpClient(
        {
            "https://example.org/api/chapters": """
            [{"chapterName":"Zeta Chapter","schoolName":"Demo University","websiteUrl":"https://demo.edu/zeta"}]
            """
        }
    )
    stubs = LocatorApiAdapter().parse_stubs(
        "<html><body><div class='storepoint-map'></div></body></html>",
        "https://example.org/chapters",
        api_url="https://example.org/api/chapters",
        http_client=http_client,
    )

    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Zeta Chapter"
    assert stubs[0].university_name == "Demo University"
    assert stubs[0].outbound_chapter_url_candidate == "https://demo.edu/zeta"




def test_script_json_adapter_extracts_mapplic_map_payload():
    html = '''
    <div id="mapplic-id187" data-mapdata="{&quot;levels&quot;:[{&quot;locations&quot;:[{&quot;id&quot;:&quot;ca&quot;,&quot;title&quot;:&quot;California&quot;,&quot;pin&quot;:&quot;hidden&quot;,&quot;category&quot;:&quot;state&quot;},{&quot;id&quot;:&quot;theta_xi_purdue&quot;,&quot;title&quot;:&quot;Theta Xi - Purdue University&quot;,&quot;pin&quot;:&quot;no-fill&quot;,&quot;category&quot;:&quot;chapter&quot;,&quot;x&quot;:&quot;0.5000&quot;,&quot;y&quot;:&quot;0.2000&quot;,&quot;description&quot;:&quot;&lt;p&gt;Chapter: Theta Xi&lt;br /&gt;University: Purdue University&lt;br /&gt;Location: West Lafayette, IN&lt;/p&gt;&quot;}]}]}"></div>
    '''

    records = ScriptJsonAdapter().parse(html, "https://ato.org/home-2/ato-map/")
    stubs = ScriptJsonAdapter().parse_stubs(html, "https://ato.org/home-2/ato-map/")

    assert len(records) == 1
    assert records[0].name == "Theta Xi"
    assert records[0].university_name == "Purdue University"
    assert records[0].city == "West Lafayette"
    assert records[0].state == "IN"
    assert records[0].external_id == "theta_xi_purdue"
    assert records[0].source_confidence >= 0.75
    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Theta Xi"
    assert stubs[0].university_name == "Purdue University"
    assert stubs[0].detail_url == "https://ato.org/home-2/ato-map/"
