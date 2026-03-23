from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from fratfinder_crawler.config import Settings
from fratfinder_crawler.search.client import SearchClient, SearchUnavailableError


def test_duckduckgo_html_search_parses_results():
    html = """
    <html><body>
      <div class="result">
        <a class="result__a" href="https://example.org/chapter">Sigma Chi at Demo University</a>
        <a class="result__snippet">Official chapter website</a>
      </div>
    </body></html>
    """
    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="duckduckgo_html"),
        get_requester=lambda url, params, timeout, verify, headers: SimpleNamespace(
            status_code=200,
            text=html,
            raise_for_status=lambda: None,
        ),
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].url == "https://example.org/chapter"
    assert results[0].title == "Sigma Chi at Demo University"


def test_brave_search_requires_api_key():
    settings = Settings(
        database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
        CRAWLER_SEARCH_PROVIDER="brave_api",
        CRAWLER_SEARCH_BRAVE_API_KEY=None,
    )
    client = SearchClient(settings)

    with pytest.raises(SearchUnavailableError):
        client.search("sigma chi demo university website")


def test_duckduckgo_anomaly_falls_back_to_bing_html():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "duckduckgo" in url:
            return SimpleNamespace(
                status_code=202,
                text="<html><body><form>Anomaly detected</form></body></html>",
                raise_for_status=lambda: None,
            )
        return SimpleNamespace(
            status_code=200,
            text="""<html><body><ol><li class="b_algo"><h2><a href="https://example.org/chapter">Sigma Chi at Demo University</a></h2><div class="b_caption"><p>Official chapter website</p></div></li></ol></body></html>""",
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="duckduckgo_html"),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "bing_html"
    assert calls[0].startswith("https://html.duckduckgo.com")
    assert calls[1].startswith("https://www.bing.com")



def test_duckduckgo_request_exception_falls_back_to_bing_html():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "duckduckgo" in url:
            raise requests.ConnectionError("duckduckgo unavailable")
        return SimpleNamespace(
            status_code=200,
            text="""<html><body><ol><li class="b_algo"><h2><a href="https://example.org/chapter">Sigma Chi at Demo University</a></h2><div class="b_caption"><p>Official chapter website</p></div></li></ol></body></html>""",
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="duckduckgo_html"),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "bing_html"
    assert calls[0].startswith("https://html.duckduckgo.com")
    assert calls[1].startswith("https://www.bing.com")


def test_bing_redirect_url_is_decoded():
    client = SearchClient(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="bing_html"), get_requester=lambda url, params, timeout, verify, headers: SimpleNamespace(
        status_code=200,
        text="""<html><body><ol><li class="b_algo"><h2><a href="https://www.bing.com/ck/a?!&&p=abc&u=a1aHR0cHM6Ly9leGFtcGxlLm9yZy9jaGFwdGVy&ntb=1">Sigma Chi at Demo University</a></h2><div class="b_caption"><p>Official chapter website</p></div></li></ol></body></html>""",
        raise_for_status=lambda: None,
    ))

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].url == "https://example.org/chapter"


def test_auto_provider_prefers_brave_when_key_present():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "api.search.brave.com" in url:
            return SimpleNamespace(
                status_code=200,
                text='{"web":{"results":[{"title":"Sigma Chi at Demo University","url":"https://example.org/chapter","description":"Official chapter website"}]}}',
                json=lambda: {"web": {"results": [{"title": "Sigma Chi at Demo University", "url": "https://example.org/chapter", "description": "Official chapter website"}]}},
                raise_for_status=lambda: None,
            )
        raise AssertionError("Bing fallback should not be used when Brave succeeds")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto",
            CRAWLER_SEARCH_BRAVE_API_KEY="test-key",
        ),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "brave_api"
    assert calls[0].startswith("https://api.search.brave.com")


def test_auto_provider_falls_back_to_bing_when_brave_unavailable():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "api.search.brave.com" in url:
            raise requests.ConnectionError("brave unavailable")
        return SimpleNamespace(
            status_code=200,
            text="""<html><body><ol><li class="b_algo"><h2><a href="https://example.org/chapter">Sigma Chi at Demo University</a></h2><div class="b_caption"><p>Official chapter website</p></div></li></ol></body></html>""",
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto",
            CRAWLER_SEARCH_BRAVE_API_KEY="test-key",
        ),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "bing_html"
    assert calls[0].startswith("https://api.search.brave.com")
    assert calls[1].startswith("https://www.bing.com")
