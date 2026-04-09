from fratfinder_crawler.discovery import discover_source
from fratfinder_crawler.models import ExistingSourceCandidate, VerifiedSourceRecord
from fratfinder_crawler.search.client import SearchResult


class StubSearchClient:
    def __init__(self, responses):
        self._responses = responses

    def search(self, query: str, max_results: int | None = None):
        return list(self._responses.get(query, []))


class CapturingStubSearchClient(StubSearchClient):
    def __init__(self, responses):
        super().__init__(responses)
        self.queries: list[str] = []

    def search(self, query: str, max_results: int | None = None):
        self.queries.append(query)
        return super().search(query, max_results=max_results)


class StubDiscoveryRepository:
    def __init__(
        self,
        verified_source: VerifiedSourceRecord | None = None,
        existing_sources: list[ExistingSourceCandidate] | None = None,
    ):
        self._verified_source = verified_source
        self._existing_sources = existing_sources or []

    def get_verified_source_by_slug(self, fraternity_slug: str) -> VerifiedSourceRecord | None:
        if self._verified_source is None:
            return None
        if self._verified_source.fraternity_slug != fraternity_slug:
            return None
        return self._verified_source

    def get_existing_source_candidates(self, fraternity_slug: str) -> list[ExistingSourceCandidate]:
        return [candidate for candidate in self._existing_sources if fraternity_slug in candidate.source_slug]


def test_discover_source_prefers_official_directory_host():
    query_results = {
        '"Lambda Chi Alpha" national fraternity website': [
            SearchResult(
                title="Lambda Chi Alpha Fraternity - Official Site",
                url="https://www.lambdachialpha.org/",
                snippet="Official international fraternity website with chapter directory.",
                provider="bing_html",
                rank=1,
            ),
            SearchResult(
                title="Wikipedia",
                url="https://en.wikipedia.org/wiki/Lambda_Chi_Alpha",
                snippet="Wikipedia entry",
                provider="bing_html",
                rank=2,
            ),
        ],
        '"Lambda Chi Alpha" chapter directory': [
            SearchResult(
                title="Find a Chapter | Lambda Chi Alpha",
                url="https://www.lambdachialpha.org/chapters/",
                snippet="Find a chapter directory.",
                provider="bing_html",
                rank=1,
            )
        ],
        '"Lambda Chi Alpha" official fraternity': [],
        '"Lambda Chi Alpha" find a chapter': [],
    }
    result = discover_source("Lambda Chi Alpha", StubSearchClient(query_results))

    assert result.fraternity_slug == "lambda-chi-alpha"
    assert result.selected_url == "https://www.lambdachialpha.org/chapters/"
    assert result.confidence_tier == "high"
    assert result.candidates[0].score >= result.candidates[-1].score


def test_discover_source_returns_low_when_no_candidates():
    result = discover_source("Example Fraternity", StubSearchClient({}))

    assert result.selected_url is None
    assert result.selected_confidence == 0.0
    assert result.confidence_tier == "low"
    assert result.candidates == []


def test_discover_source_is_deterministic_for_same_input():
    responses = {
        '"Delta Chi" national fraternity website': [
            SearchResult(
                title="Delta Chi Official",
                url="https://www.deltachi.org/",
                snippet="Official site",
                provider="bing_html",
                rank=1,
            )
        ],
        '"Delta Chi" chapter directory': [
            SearchResult(
                title="Delta Chi Chapter Directory",
                url="https://www.deltachi.org/chapter-directory/",
                snippet="Directory",
                provider="bing_html",
                rank=1,
            )
        ],
        '"Delta Chi" official fraternity': [],
        '"Delta Chi" find a chapter': [],
    }

    client = StubSearchClient(responses)
    left = discover_source("Delta Chi", client)
    right = discover_source("Delta Chi", client)

    assert left.selected_url == right.selected_url
    assert left.selected_confidence == right.selected_confidence
    assert [item.url for item in left.candidates] == [item.url for item in right.candidates]


def test_discover_source_rejects_low_confidence_non_fraternity_hits():
    responses = {
        '"Lambda Chi Alpha" national fraternity website': [
            SearchResult(
                title="What is a lambda function?",
                url="https://stackoverflow.com/questions/16501/what-is-a-lambda-function",
                snippet="Programming question and answers about lambda functions.",
                provider="bing_html",
                rank=1,
            )
        ],
        '"Lambda Chi Alpha" chapter directory': [],
        '"Lambda Chi Alpha" official fraternity': [],
        '"Lambda Chi Alpha" find a chapter': [],
    }

    result = discover_source("Lambda Chi Alpha", StubSearchClient(responses))

    assert result.candidates
    assert result.candidates[0].score < 0.6
    assert result.selected_url is None
    assert result.selected_confidence == 0.0
    assert result.confidence_tier == "low"


def test_discover_source_uses_alias_query_for_phi_gamma_delta_fiji():
    responses = {
        '"Phi Gamma Delta" national fraternity website': [],
        '"Phi Gamma Delta" chapter directory': [],
        '"Phi Gamma Delta" official fraternity': [],
        '"Phi Gamma Delta" find a chapter': [],
        '"fiji" fraternity national website': [
            SearchResult(
                title="Phi Gamma Delta Fraternity",
                url="https://phigam.org/",
                snippet="Official fraternity website",
                provider="bing_html",
                rank=1,
            )
        ],
        '"fiji" chapter directory': [],
    }
    result = discover_source("Phi Gamma Delta", StubSearchClient(responses))

    assert result.selected_url == "https://phigam.org/about/overview/our-chapters/"
    assert result.confidence_tier in {"high", "medium"}


def test_discover_source_prefers_phigam_over_fiji_travel_noise():
    responses = {
        '"Phi Gamma Delta" national fraternity website': [],
        '"Phi Gamma Delta" chapter directory': [],
        '"Phi Gamma Delta" official fraternity': [],
        '"Phi Gamma Delta" find a chapter': [],
        '"fiji" fraternity national website': [
            SearchResult(
                title="Fiji Travel Packages",
                url="https://www.fiji.travel/",
                snippet="Vacation and travel packages in Fiji islands.",
                provider="bing_html",
                rank=1,
            ),
            SearchResult(
                title="Phi Gamma Delta | Official Fraternity",
                url="https://www.phigam.org/",
                snippet="Official Phi Gamma Delta fraternity chapter directory.",
                provider="bing_html",
                rank=2,
            ),
        ],
        '"fiji" chapter directory': [],
    }

    result = discover_source("Phi Gamma Delta", StubSearchClient(responses))

    assert result.selected_url == "https://www.phigam.org/"
    assert result.candidates
    assert result.candidates[0].url == "https://www.phigam.org/"


def test_discover_source_adds_host_hint_queries_for_phi_gamma_delta():
    client = CapturingStubSearchClient({})
    discover_source("Phi Gamma Delta", client)

    assert '"Phi Gamma Delta" "phigam.org" fraternity' in client.queries
    assert '"phigam.org" chapter directory fraternity' in client.queries


def test_discover_source_uses_curated_source_hint_when_search_is_noisy():
    responses = {
        '"Phi Gamma Delta" national fraternity website': [
            SearchResult(
                title="What is PHI?",
                url="https://www.hhs.gov/answers/hipaa/what-is-phi/index.html",
                snippet="HIPAA PHI information",
                provider="bing_html",
                rank=1,
            )
        ],
        '"Phi Gamma Delta" chapter directory': [],
        '"Phi Gamma Delta" official fraternity': [],
        '"Phi Gamma Delta" find a chapter': [],
        '"Phi Gamma Delta" "phigam.org" fraternity': [],
        '"phigam.org" chapter directory fraternity': [],
        '"fiji" fraternity national website': [],
        '"fiji" chapter directory': [],
    }

    result = discover_source("Phi Gamma Delta", StubSearchClient(responses))

    assert result.selected_url == "https://phigam.org/about/overview/our-chapters/"
    assert result.selected_confidence >= 0.8
    assert result.confidence_tier == "high"


def test_discover_source_prefers_theta_chi_official_host_and_recovers_chapters_page():
    responses = {
        '"Theta Chi" national fraternity website': [
            SearchResult(
                title="Theta Chi Fraternity | About",
                url="https://www.thetachi.org/about",
                snippet="Official Theta Chi fraternity history and brotherhood.",
                provider="bing_html",
                rank=1,
            ),
            SearchResult(
                title="Kappa Kappa Psi Chapter Listing",
                url="https://www.kkpsi.org/about/chapters-districts/chapter-listing-2/",
                snippet="Official Kappa Kappa Psi chapters and districts listing.",
                provider="bing_html",
                rank=2,
            ),
        ],
        '"Theta Chi" chapter directory': [],
        '"Theta Chi" chapter list': [],
        '"Theta Chi" chapters': [],
        '"Theta Chi" official fraternity': [],
        '"Theta Chi" find a chapter': [],
        '"Theta Chi" active chapters': [],
        '"Theta Chi" chapter roll': [],
    }

    html_by_url = {
        "https://www.thetachi.org/about": """
        <html><body>
          <nav>
            <a href="/staff-directory">Staff Directory</a>
            <a href="/chapters">Chapters</a>
          </nav>
        </body></html>
        """
    }

    result = discover_source(
        "Theta Chi",
        StubSearchClient(responses),
        html_fetcher=lambda url: html_by_url.get(url),
    )

    assert result.selected_url == "https://www.thetachi.org/chapters"
    assert result.selected_candidate_rationale == "recovered_same_host_directory_link"
    assert not any(candidate.url == "https://www.kkpsi.org/about/chapters-districts/chapter-listing-2/" and candidate.score >= 0.6 for candidate in result.candidates)


def test_discover_source_uses_verified_registry_before_search():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="lambda-chi-alpha",
            fraternity_name="Lambda Chi Alpha",
            national_url="https://www.lambdachialpha.org/chapters/",
            origin="nic_bootstrap",
            confidence=0.92,
            http_status=200,
            checked_at="2026-03-31T00:00:00+00:00",
            is_active=True,
            metadata={},
        )
    )
    client = CapturingStubSearchClient({})

    result = discover_source("Lambda Chi Alpha", client, repository=repository)

    assert result.selected_url == "https://www.lambdachialpha.org/chapters/"
    assert result.source_provenance == "verified_registry"
    assert result.confidence_tier == "high"
    assert client.queries == []


def test_discover_source_falls_back_to_existing_source_when_registry_unhealthy():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="lambda-chi-alpha",
            fraternity_name="Lambda Chi Alpha",
            national_url="https://www.lambdachialpha.org/chapters/",
            origin="nic_bootstrap",
            confidence=0.55,
            http_status=503,
            checked_at="2026-03-31T00:00:00+00:00",
            is_active=False,
            metadata={},
        ),
        existing_sources=[
            ExistingSourceCandidate(
                source_slug="lambda-chi-alpha-main",
                list_url="https://chapterbuilder.lambdachialpha.org/chapters",
                base_url="https://chapterbuilder.lambdachialpha.org",
                source_type="html_directory",
                parser_key="directory_v1",
                active=True,
                last_run_status="succeeded",
                last_success_at="2026-03-30T00:00:00+00:00",
                confidence=0.9,
            )
        ],
    )
    client = CapturingStubSearchClient({})

    result = discover_source("Lambda Chi Alpha", client, repository=repository)

    assert result.selected_url == "https://chapterbuilder.lambdachialpha.org/chapters"
    assert result.source_provenance == "existing_source"
    assert client.queries == []


def test_discover_source_conflict_policy_prefers_existing_when_healthier():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="phi-gamma-delta",
            fraternity_name="Phi Gamma Delta",
            national_url="https://phigam.org/about/overview/our-chapters/",
            origin="nic_bootstrap",
            confidence=0.83,
            http_status=200,
            checked_at="2026-01-01T00:00:00+00:00",
            is_active=True,
            metadata={},
        ),
        existing_sources=[
            ExistingSourceCandidate(
                source_slug="phi-gamma-delta-main",
                list_url="https://phigam.org/chapter-directory/",
                base_url="https://phigam.org",
                source_type="html_directory",
                parser_key="directory_v1",
                active=True,
                last_run_status="succeeded",
                last_success_at="2026-03-30T00:00:00+00:00",
                confidence=0.95,
            )
        ],
    )
    client = CapturingStubSearchClient({})

    result = discover_source("Phi Gamma Delta", client, repository=repository)

    assert result.selected_url == "https://phigam.org/chapter-directory/"
    assert result.source_provenance == "existing_source"
    assert result.fallback_reason == "registry_disagreed_preferred_existing_source"


def test_discover_source_rejects_invalid_existing_source_and_falls_back_to_search():
    repository = StubDiscoveryRepository(
        existing_sources=[
            ExistingSourceCandidate(
                source_slug="lambda-chi-alpha-main",
                list_url="https://stackoverflow.com/questions/16501/what-is-a-lambda-function",
                base_url="https://stackoverflow.com",
                source_type="unsupported",
                parser_key="unsupported",
                active=True,
                last_run_status="succeeded",
                last_success_at="2026-03-30T00:00:00+00:00",
                confidence=0.9,
            )
        ],
    )
    responses = {
        '"Lambda Chi Alpha" national fraternity website': [
            SearchResult(
                title="Lambda Chi Alpha Fraternity - Official Site",
                url="https://www.lambdachialpha.org/",
                snippet="Official Lambda Chi Alpha fraternity website with chapter directory.",
                provider="searxng_json",
                rank=1,
            )
        ],
        '"Lambda Chi Alpha" fraternity national website': [],
        '"Lambda Chi Alpha" chapter directory': [],
        '"Lambda Chi Alpha" official fraternity': [],
        '"Lambda Chi Alpha" find a chapter': [],
        '"Lambda Chi Alpha" chapter roll': [],
    }

    result = discover_source("Lambda Chi Alpha", StubSearchClient(responses), repository=repository)

    assert result.selected_url == "https://www.lambdachialpha.org/"
    assert result.source_provenance == "search"
    assert result.fallback_reason == "existing_source_invalid"


def test_discover_source_rejects_weak_verified_member_path_and_uses_curated_hint():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="sigma-chi",
            fraternity_name="Sigma Chi",
            national_url="https://members.sigmachi.org/alumnigroups",
            origin="nic_bootstrap",
            confidence=0.95,
            http_status=200,
            checked_at="2026-04-01T00:00:00+00:00",
            is_active=True,
            metadata={},
        )
    )

    result = discover_source("Sigma Chi", StubSearchClient({}), repository=repository)

    assert result.selected_url == "https://sigmachi.org/chapters/"
    assert result.source_provenance == "search"
    assert result.fallback_reason == "verified_source_invalid"
    assert any(step.get("step") == "rejected_verified_registry_candidate" for step in result.resolution_trace)


def test_discover_source_rejects_weak_existing_member_path_and_uses_curated_hint():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="sigma-chi",
            fraternity_name="Sigma Chi",
            national_url="https://members.sigmachi.org/alumnigroups",
            origin="nic_bootstrap",
            confidence=0.95,
            http_status=200,
            checked_at="2026-04-01T00:00:00+00:00",
            is_active=True,
            metadata={},
        ),
        existing_sources=[
            ExistingSourceCandidate(
                source_slug="sigma-chi-main",
                list_url="https://members.sigmachi.org/alumnigroups",
                base_url="https://members.sigmachi.org",
                source_type="html_directory",
                parser_key="directory_v1",
                active=True,
                last_run_status="partial",
                last_success_at="2026-03-30T00:00:00+00:00",
                confidence=0.75,
            )
        ],
    )

    result = discover_source("Sigma Chi", StubSearchClient({}), repository=repository)

    assert result.selected_url == "https://sigmachi.org/chapters/"
    assert result.source_provenance == "search"
    assert result.fallback_reason == "verified_source_invalid"
    assert any(step.get("step") == "rejected_existing_source_candidate" for step in result.resolution_trace)


def test_discover_source_rejects_existing_source_with_no_success_history_and_uses_theta_xi_hint():
    repository = StubDiscoveryRepository(
        existing_sources=[
            ExistingSourceCandidate(
                source_slug="theta-xi-main",
                list_url="https://thetaxi.dynamic.omegafi.com/mythetaxi/",
                base_url="https://thetaxi.dynamic.omegafi.com",
                source_type="html_directory",
                parser_key="directory_v1",
                active=True,
                last_run_status="partial",
                last_success_at=None,
                confidence=0.75,
            )
        ],
    )

    result = discover_source("Theta Xi", StubSearchClient({}), repository=repository)

    assert result.selected_url == "https://www.thetaxi.org/chapters-and-colonies/"
    assert result.source_provenance == "search"
    assert result.selected_candidate_rationale == "curated_hint_safe_fallback"
    rejected_step = next(
        step for step in result.resolution_trace if step.get("step") == "rejected_existing_source_candidate"
    )
    assert "no_success_history" in (rejected_step.get("reasons") or [])


def test_discover_source_uses_curated_hint_over_noisy_alumni_search_result():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="sigma-chi",
            fraternity_name="Sigma Chi",
            national_url="https://members.sigmachi.org/alumnigroups",
            origin="nic_bootstrap",
            confidence=0.95,
            http_status=200,
            checked_at="2026-04-01T00:00:00+00:00",
            is_active=True,
            metadata={},
        ),
        existing_sources=[
            ExistingSourceCandidate(
                source_slug="sigma-chi-main",
                list_url="https://members.sigmachi.org/alumnigroups",
                base_url="https://members.sigmachi.org",
                source_type="html_directory",
                parser_key="directory_v1",
                active=True,
                last_run_status="partial",
                last_success_at="2026-03-30T00:00:00+00:00",
                confidence=0.75,
            )
        ],
    )
    responses = {
        '"Sigma Chi" national fraternity website': [
            SearchResult(
                title="San Diego Sigma Chi Alumni Chapter",
                url="https://www.sandiegosigmachi.org/",
                snippet="The San Diego Sigma Chi Alumni Chapter serves alumni in Southern California.",
                provider="searxng_json",
                rank=1,
            )
        ],
        '"Sigma Chi" fraternity national website': [],
        '"Sigma Chi" chapter directory': [],
        '"Sigma Chi" official fraternity': [],
        '"Sigma Chi" find a chapter': [],
        '"Sigma Chi" chapter roll': [],
        '"Sigma Chi" "sigmachi.org" fraternity': [],
        '"sigmachi.org" chapter directory fraternity': [],
    }

    result = discover_source("Sigma Chi", StubSearchClient(responses), repository=repository)

    assert result.selected_url == "https://sigmachi.org/chapters/"
    assert result.source_provenance == "search"
    assert result.fallback_reason == "verified_source_invalid"
    assert any(step.get("step") == "selected_curated_source_hint_over_noisy_search" for step in result.resolution_trace)


def test_discover_source_uses_curated_hint_over_generic_same_host_page():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="sigma-chi",
            fraternity_name="Sigma Chi",
            national_url="https://members.sigmachi.org/alumnigroups",
            origin="nic_bootstrap",
            confidence=0.95,
            http_status=200,
            checked_at="2026-04-01T00:00:00+00:00",
            is_active=True,
            metadata={},
        ),
        existing_sources=[
            ExistingSourceCandidate(
                source_slug="sigma-chi-main",
                list_url="https://members.sigmachi.org/alumnigroups",
                base_url="https://members.sigmachi.org",
                source_type="html_directory",
                parser_key="directory_v1",
                active=True,
                last_run_status="partial",
                last_success_at="2026-03-30T00:00:00+00:00",
                confidence=0.75,
            )
        ],
    )
    responses = {
        '"Sigma Chi" national fraternity website': [
            SearchResult(
                title="History - Sigma Chi",
                url="https://sigmachi.org/history/",
                snippet="The history of Sigma Chi and its traditions.",
                provider="searxng_json",
                rank=1,
            )
        ],
        '"Sigma Chi" fraternity national website': [],
        '"Sigma Chi" chapter directory': [],
        '"Sigma Chi" official fraternity': [],
        '"Sigma Chi" find a chapter': [],
        '"Sigma Chi" chapter roll': [],
        '"Sigma Chi" "sigmachi.org" fraternity': [],
        '"sigmachi.org" chapter directory fraternity': [],
    }

    result = discover_source("Sigma Chi", StubSearchClient(responses), repository=repository)

    assert result.selected_url == "https://sigmachi.org/chapters/"
    assert result.source_provenance == "search"
    assert any(step.get("step") == "selected_curated_source_hint_over_generic_same_host_page" for step in result.resolution_trace)


def test_discover_source_keeps_verified_root_when_not_obviously_weak():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="chi-psi",
            fraternity_name="Chi Psi",
            national_url="https://chipsi.org/",
            origin="nic_bootstrap",
            confidence=0.85,
            http_status=200,
            checked_at="2026-04-01T00:00:00+00:00",
            is_active=True,
            metadata={},
        )
    )

    result = discover_source("Chi Psi", StubSearchClient({}), repository=repository)

    assert result.selected_url == "https://chipsi.org/"
    assert result.source_provenance == "verified_registry"

def test_discover_source_prefers_curated_ato_map_over_generic_root():
    repository = StubDiscoveryRepository(
        verified_source=VerifiedSourceRecord(
            fraternity_slug="alpha-tau-omega",
            fraternity_name="Alpha Tau Omega",
            national_url="https://ato.org/",
            origin="nic_bootstrap",
            confidence=0.9,
            http_status=200,
            checked_at="2026-04-01T00:00:00+00:00",
            is_active=True,
            metadata={},
        )
    )

    result = discover_source("Alpha Tau Omega", StubSearchClient({}), repository=repository)

    assert result.selected_url == "https://ato.org/home-2/ato-map/"
    assert result.source_provenance == "search"
    assert result.fallback_reason == "verified_source_invalid"
    assert any(step.get("step") == "selected_curated_hint" for step in result.resolution_trace)


def test_discover_source_prefers_generic_chapter_list_over_about_page_without_hint():
    responses = {
        '"Theta Chi" national fraternity website': [
            SearchResult(
                title="About | Theta Chi",
                url="https://www.thetachi.org/about",
                snippet="Learn about Theta Chi fraternity and its history.",
                provider="brave_html",
                rank=1,
            )
        ],
        '"Theta Chi" fraternity national website': [],
        '"Theta Chi" chapter directory': [],
        '"Theta Chi" chapter list': [
            SearchResult(
                title="Chapters | Theta Chi",
                url="https://www.thetachi.org/chapters",
                snippet="Browse active chapters and chapter pages across the fraternity.",
                provider="brave_html",
                rank=1,
            )
        ],
        '"Theta Chi" chapters': [],
        '"Theta Chi" official fraternity': [],
        '"Theta Chi" find a chapter': [],
        '"Theta Chi" active chapters': [],
        '"Theta Chi" chapter roll': [],
        '"TC" national fraternity website': [],
        '"TC" fraternity national website': [],
        '"TC" chapter directory': [],
        '"TC" chapter list': [],
        '"TC" chapters': [],
        '"TC" official fraternity': [],
        '"TC" find a chapter': [],
        '"TC" active chapters': [],
        '"TC" chapter roll': [],
    }

    result = discover_source("Theta Chi", StubSearchClient(responses))

    assert result.selected_url == "https://www.thetachi.org/chapters"
    assert result.source_provenance == "search"
    assert result.selected_candidate_rationale == "selected_search_candidate"


def test_discover_source_recovers_same_host_chapter_link_from_generic_about_page():
    responses = {
        '"Theta Chi" national fraternity website': [
            SearchResult(
                title="About | Theta Chi",
                url="https://www.thetachi.org/about",
                snippet="Learn about Theta Chi fraternity and its history.",
                provider="brave_html",
                rank=1,
            )
        ],
        '"Theta Chi" fraternity national website': [],
        '"Theta Chi" chapter directory': [],
        '"Theta Chi" chapter list': [],
        '"Theta Chi" chapters': [],
        '"Theta Chi" official fraternity': [],
        '"Theta Chi" find a chapter': [],
        '"Theta Chi" active chapters': [],
        '"Theta Chi" chapter roll': [],
        '"TC" national fraternity website': [],
        '"TC" fraternity national website': [],
        '"TC" chapter directory': [],
        '"TC" chapter list': [],
        '"TC" chapters': [],
        '"TC" official fraternity': [],
        '"TC" find a chapter': [],
        '"TC" active chapters': [],
        '"TC" chapter roll': [],
    }

    def fetcher(_: str) -> str:
        return """
        <html>
          <body>
            <nav>
              <a href="/about">About</a>
              <a href="/chapters">Chapters</a>
              <a href="/contact">Contact</a>
            </nav>
          </body>
        </html>
        """

    result = discover_source("Theta Chi", StubSearchClient(responses), html_fetcher=fetcher)

    assert result.selected_url == "https://www.thetachi.org/chapters"
    assert result.selected_candidate_rationale == "recovered_same_host_directory_link"
    assert any(step.get("step") == "recovered_same_host_directory_link" for step in result.resolution_trace)


def test_discover_source_same_host_recovery_prefers_chapters_over_staff_directory():
    responses = {
        '"Theta Chi" national fraternity website': [
            SearchResult(
                title="Theta Chi",
                url="https://www.thetachi.org/",
                snippet="Theta Chi fraternity official website.",
                provider="brave_html",
                rank=1,
            )
        ],
        '"Theta Chi" fraternity national website': [],
        '"Theta Chi" chapter directory': [],
        '"Theta Chi" chapter list': [],
        '"Theta Chi" chapters': [],
        '"Theta Chi" official fraternity': [],
        '"Theta Chi" find a chapter': [],
        '"Theta Chi" active chapters': [],
        '"Theta Chi" chapter roll': [],
        '"TC" national fraternity website': [],
        '"TC" fraternity national website': [],
        '"TC" chapter directory': [],
        '"TC" chapter list': [],
        '"TC" chapters': [],
        '"TC" official fraternity': [],
        '"TC" find a chapter': [],
        '"TC" active chapters': [],
        '"TC" chapter roll': [],
    }

    def fetcher(_: str) -> str:
        return """
        <html>
          <body>
            <nav>
              <a href="/staff-directory">Staff Directory</a>
              <a href="/chapters">Chapters</a>
              <a href="/expansion">Start a Chapter</a>
            </nav>
          </body>
        </html>
        """

    result = discover_source("Theta Chi", StubSearchClient(responses), html_fetcher=fetcher)

    assert result.selected_url == "https://www.thetachi.org/chapters"
    assert result.selected_candidate_rationale == "recovered_same_host_directory_link"

