from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.analysis import score_chapter_link
from fratfinder_crawler.models import EmbeddedDataResult, PageAnalysis, SourceClassification
from fratfinder_crawler.orchestration.navigation import (
    classify_chapter_target,
    detect_chapter_index_mode,
    extract_chapter_stubs,
    extract_contacts_from_chapter_site,
    follow_chapter_detail_or_outbound,
)


class RoutingHttpClient:
    def __init__(self, responses: dict[str, str]) -> None:
        self._responses = responses

    def get(self, url: str) -> str:
        if url not in self._responses:
            raise AssertionError(f"Unexpected URL requested: {url}")
        return self._responses[url]


def test_score_chapter_link_rewards_chapter_directory_cta():
    score = score_chapter_link("Go To Site", "https://example.org/chapter-directory/alpha", "Alpha Chapter University of Example")
    assert score.score >= 0.7
    assert score.positive_reasons


def test_detect_chapter_index_mode_identifies_map_locator():
    mode, confidence, reason = detect_chapter_index_mode(
        "<html><body><iframe src='https://www.google.com/maps/d/u/0/embed?mid=abc'></iframe></body></html>",
        PageAnalysis(
            title="Map",
            headings=["Map"],
            table_count=0,
            repeated_block_count=0,
            link_count=2,
            has_json_ld=False,
            has_script_json=False,
            has_map_widget=True,
            has_pagination=False,
            probable_page_role="search",
            text_sample="map",
        ),
        SourceClassification(
            page_type="locator_map",
            confidence=0.8,
            recommended_strategy="locator_api",
            needs_follow_links=False,
            possible_data_locations=["map_widget"],
            classified_by="heuristic",
        ),
        EmbeddedDataResult(found=True, data_type="api_hint", raw_data=None, api_url="https://example.org/api/chapters"),
    )

    assert mode == "map_or_api_locator"
    assert confidence >= 0.8
    assert reason == "locator_or_api_hint"


def test_extract_chapter_stubs_uses_directory_adapter_contract():
    html = """
    <html>
      <body>
        <li class="chapter-item" data-chapter-card>
          <h3 class="chapter-name">Alpha Chapter</h3>
          <div class="university">Example University</div>
          <a href="/chapters/alpha">Go To Site</a>
        </li>
      </body>
    </html>
    """
    stubs = extract_chapter_stubs(
        registry=AdapterRegistry(),
        html=html,
        source_url="https://example.org/chapters",
        mode="direct_chapter_list",
        embedded_data=EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        http_client=RoutingHttpClient({}),
    )

    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Alpha Chapter"
    assert stubs[0].university_name == "Example University"
    assert stubs[0].outbound_chapter_url_candidate == "https://example.org/chapters/alpha"


def test_extract_chapter_stubs_reads_map_config_state_urls():
    html = """
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
    stubs = extract_chapter_stubs(
        registry=AdapterRegistry(),
        html=html,
        source_url="https://example.org/chapters",
        mode="map_or_api_locator",
        embedded_data=EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        http_client=RoutingHttpClient({}),
    )

    assert len(stubs) == 1
    assert stubs[0].chapter_name == "Mississippi"
    assert stubs[0].detail_url == "https://example.org/chapters/mississippi/"


def test_follow_and_extract_contacts_respects_budget_and_extracts_fields():
    html = """
    <html><body>
      Contact us at brothers@example.edu.
      Follow us https://www.instagram.com/examplechapter/
    </body></html>
    """
    stubs = extract_chapter_stubs(
        registry=AdapterRegistry(),
        html="""
        <li class="chapter-item" data-chapter-card>
          <h3 class="chapter-name">Beta Chapter</h3>
          <a href="https://chapters.example.edu/beta">Chapter Website</a>
        </li>
        """,
        source_url="https://example.org/chapters",
        mode="direct_chapter_list",
        embedded_data=EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        http_client=RoutingHttpClient({}),
    )
    pages_by_stub, stats = follow_chapter_detail_or_outbound(
        stubs=stubs,
        source_url="https://example.org/chapters",
        http_client=RoutingHttpClient({"https://chapters.example.edu/beta": html}),
        max_hops_per_stub=2,
        max_pages_per_run=5,
    )
    hints = extract_contacts_from_chapter_site(stubs, pages_by_stub)

    assert stats["fetched_pages"] == 1
    assert hints
    first = next(iter(hints.values()))
    assert first["email"] == "brothers@example.edu"
    assert first["instagram_url"] == "https://www.instagram.com/examplechapter/"


def test_classify_chapter_target_marks_external_chapter_site_as_not_followable():
    target = classify_chapter_target(
        source_url="https://phigam.org/about/overview/our-chapters/",
        candidate_url="http://www.fijiuwo.com/",
    )

    assert target.target_type == "chapter_owned_site"
    assert target.follow_allowed is False
    assert target.rejection_reason == "chapter_site_only"


def test_classify_chapter_target_blocks_personal_homepage_style_institutional_urls():
    target = classify_chapter_target(
        source_url="https://phigam.org/about/overview/our-chapters/",
        candidate_url="http://www.student.richmond.edu/~fiji",
    )

    assert target.target_type == "institutional_page"
    assert target.follow_allowed is False
    assert target.rejection_reason == "external_target_timeout_risk"


def test_follow_chapter_detail_or_outbound_skips_external_chapter_sites_when_disabled():
    stubs = extract_chapter_stubs(
        registry=AdapterRegistry(),
        html="""
        <li class="chapter-item" data-chapter-card>
          <h3 class="chapter-name">Lambda Omega Chapter</h3>
          <div class="university">Western Example University</div>
          <a href="http://www.fijiuwo.com/">Chapter Website</a>
        </li>
        """,
        source_url="https://phigam.org/about/overview/our-chapters/",
        mode="map_or_api_locator",
        embedded_data=EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        http_client=RoutingHttpClient({}),
    )

    pages_by_stub, stats = follow_chapter_detail_or_outbound(
        stubs=stubs,
        source_url="https://phigam.org/about/overview/our-chapters/",
        http_client=RoutingHttpClient({}),
        max_hops_per_stub=2,
        max_pages_per_run=5,
        follow_external_chapter_sites=False,
    )

    assert pages_by_stub == {}
    assert stats["fetched_pages"] == 0
    assert stats["skipped_by_target_type"]["chapter_owned_site"] >= 1


def test_follow_chapter_detail_or_outbound_skips_institutional_follow_when_identity_complete():
    stubs = extract_chapter_stubs(
        registry=AdapterRegistry(),
        html="""
        <li class="chapter-item" data-chapter-card>
          <h3 class="chapter-name">Lambda Omega Chapter</h3>
          <div class="university">University of Richmond</div>
          <a href="http://www.student.richmond.edu/~fiji">Chapter Website</a>
        </li>
        """,
        source_url="https://phigam.org/about/overview/our-chapters/",
        mode="map_or_api_locator",
        embedded_data=EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        http_client=RoutingHttpClient({}),
    )

    pages_by_stub, stats = follow_chapter_detail_or_outbound(
        stubs=stubs,
        source_url="https://phigam.org/about/overview/our-chapters/",
        http_client=RoutingHttpClient({}),
        max_hops_per_stub=2,
        max_pages_per_run=5,
        follow_external_chapter_sites=False,
        allow_institutional_follow=True,
    )

    assert pages_by_stub == {}
    assert stats["fetched_pages"] == 0
    assert stats["skipped_by_target_type"]["institutional_page"] >= 1
    decisions = stats["target_decisions"]
    assert decisions
    assert decisions[0]["rejectionReason"] in {"institutional_completion_not_needed", "external_target_timeout_risk"}

def test_extract_chapter_stubs_skips_navigation_noise_in_chapter_roll_fallback():
    html = """
    <html>
      <body>
        <div>
          ORG QUICK LINKS DONATE SIGMA CHI ONLINE CHI CHAPTER SYSTEM MEMBER DEVELOPMENT
          ALUMNI OUR HISTORY SCHOLARSHIPS CONTACT US CAREERS PRIVACY STATEMENT
          REQUEST A PROGRAM SIGMA CHI FRATERNITY FOUNDATION HISTORY LEADERSHIP INSTITUTE CHAPTER UNIVERSITY
        </div>
      </body>
    </html>
    """
    stubs = extract_chapter_stubs(
        registry=AdapterRegistry(),
        html=html,
        source_url="https://sigmachi.org/chapters/",
        mode="direct_chapter_list",
        embedded_data=EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        http_client=RoutingHttpClient({}),
    )

    assert stubs == []


def test_extract_chapter_stubs_skips_irrelevant_same_host_targets():
    html = """
    <html>
      <body>
        <a href="/careers/">Alpha Chapter</a>
        <section>
          <h3>Alpha Chapter</h3>
          <div>Example University</div>
          <a href="/chapters/alpha/">Chapter Website</a>
        </section>
      </body>
    </html>
    """
    stubs = extract_chapter_stubs(
        registry=AdapterRegistry(),
        html=html,
        source_url="https://example.org/find-a-chapter",
        mode="direct_chapter_list",
        embedded_data=EmbeddedDataResult(found=False, data_type=None, raw_data=None, api_url=None),
        http_client=RoutingHttpClient({}),
    )

    assert len(stubs) == 1
    assert stubs[0].detail_url == "https://example.org/chapters/alpha/"
