from fratfinder_crawler.discovery import discover_source
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
    assert result.selected_url == "https://www.lambdachialpha.org/"
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

    assert result.selected_url == "https://phigam.org/"
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
