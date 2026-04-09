from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from fratfinder_crawler.config import Settings
from fratfinder_crawler.search.client import SearchClient, SearchUnavailableError


def test_duckduckgo_html_search_parses_results():
    html = """
    <html><body>
      <table>
        <tr>
          <td><a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.org%2Fchapter">Sigma Chi at Demo University</a></td>
        </tr>
      </table>
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
    assert calls[0].startswith("https://lite.duckduckgo.com")
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
    assert calls[0].startswith("https://lite.duckduckgo.com")
    assert calls[1].startswith("https://www.bing.com")


def test_bing_low_signal_results_fall_back_to_duckduckgo():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "bing.com" in url:
            return SimpleNamespace(
                status_code=200,
                text=(
                    "<html><body><ol>"
                    '<li class="b_algo"><h2><a href="https://www.reddit.com/r/EnglishLearning/comments/example">What does sigma mean</a></h2><div class="b_caption"><p>sigma slang thread</p></div></li>'
                    "</ol></body></html>"
                ),
                raise_for_status=lambda: None,
            )
        return SimpleNamespace(
            status_code=200,
            text="""
            <html><body>
              <table>
                <tr><td><a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.instagram.com%2Fsigmachiuchicago%2F">UChicago Sigma Chi (@sigmachiuchicago) - Instagram</a></td></tr>
              </table>
            </body></html>
            """,
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="bing_html"),
        get_requester=requester,
    )

    results = client.search('"sigma chi" University of Chicago instagram')

    assert len(results) == 1
    assert results[0].provider == "duckduckgo_html"
    assert results[0].url == "https://www.instagram.com/sigmachiuchicago/"
    assert calls[0].startswith("https://www.bing.com")
    assert calls[1].startswith("https://lite.duckduckgo.com")


def test_bing_challenge_page_falls_back_to_duckduckgo():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "bing.com" in url:
            return SimpleNamespace(
                status_code=200,
                text="<html><body><a href=\"/challenge/verify\">verify</a><div id=\"b_captcha\"></div></body></html>",
                raise_for_status=lambda: None,
            )
        return SimpleNamespace(
            status_code=200,
            text=(
                "<html><body><table>"
                "<tr><td><a href=\"//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.instagram.com%2Fsigmachiuchicago%2F\">UChicago Sigma Chi</a></td></tr>"
                "</table></body></html>"
            ),
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="bing_html"),
        get_requester=requester,
    )

    results = client.search('"sigma chi" University of Chicago instagram')

    assert len(results) == 1
    assert results[0].provider == "duckduckgo_html"
    assert calls[0].startswith("https://www.bing.com")
    assert calls[1].startswith("https://lite.duckduckgo.com")


def test_bing_and_duckduckgo_fail_then_falls_back_to_brave_html():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "bing.com" in url:
            return SimpleNamespace(
                status_code=200,
                text="<html><body><a href=\"/challenge/verify\">verify</a><div id=\"b_captcha\"></div></body></html>",
                raise_for_status=lambda: None,
            )
        if "duckduckgo" in url:
            raise requests.ConnectionError("duckduckgo unavailable")
        if "search.brave.com" in url:
            return SimpleNamespace(
                status_code=200,
                text=(
                    "<html><body>"
                    '<div class="result-wrapper"><a href="https://www.instagram.com/sigmachiuchicago/">UChicago Sigma Chi</a>'
                    "<p>Instagram profile</p></div>"
                    "</body></html>"
                ),
                raise_for_status=lambda: None,
            )
        raise AssertionError(f"Unexpected URL call: {url}")

    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="bing_html"),
        get_requester=requester,
    )

    results = client.search('"sigma chi" University of Chicago instagram')

    assert len(results) == 1
    assert results[0].provider == "brave_html"
    assert results[0].url == "https://www.instagram.com/sigmachiuchicago/"
    assert calls[0].startswith("https://www.bing.com")
    assert calls[1].startswith("https://lite.duckduckgo.com")
    assert calls[2].startswith("https://search.brave.com")


def test_bing_redirect_url_is_decoded():
    client = SearchClient(Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="bing_html"), get_requester=lambda url, params, timeout, verify, headers: SimpleNamespace(
        status_code=200,
        text="""<html><body><ol><li class="b_algo"><h2><a href="https://www.bing.com/ck/a?!&&p=abc&u=a1aHR0cHM6Ly9leGFtcGxlLm9yZy9jaGFwdGVy&ntb=1">Sigma Chi at Demo University</a></h2><div class="b_caption"><p>Official chapter website</p></div></li></ol></body></html>""",
        raise_for_status=lambda: None,
    ))

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].url == "https://example.org/chapter"


def test_auto_provider_prefers_searxng_before_brave_when_available():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "localhost:8888/search" in url:
            return SimpleNamespace(
                status_code=200,
                text='{"results":[{"title":"Sigma Chi at Demo University","url":"https://example.org/chapter","content":"Official chapter website"}]}',
                json=lambda: {"results": [{"title": "Sigma Chi at Demo University", "url": "https://example.org/chapter", "content": "Official chapter website"}]},
                raise_for_status=lambda: None,
            )
        raise AssertionError("Fallback providers should not be used when SearXNG succeeds")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto",
            CRAWLER_SEARCH_BRAVE_API_KEY="test-key",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
        ),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "searxng_json"
    assert calls == ["http://localhost:8888/search"]


def test_auto_provider_falls_back_to_brave_when_searxng_unavailable():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "localhost:8888/search" in url:
            raise requests.ConnectionError("searx unavailable")
        if "api.search.brave.com" in url:
            return SimpleNamespace(
                status_code=200,
                text='{"web":{"results":[{"title":"Sigma Chi at Demo University","url":"https://example.org/chapter","description":"Official chapter website"}]}}',
                json=lambda: {"web": {"results": [{"title": "Sigma Chi at Demo University", "url": "https://example.org/chapter", "description": "Official chapter website"}]}},
                raise_for_status=lambda: None,
            )
        raise AssertionError(f"Unexpected fallback URL: {url}")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto",
            CRAWLER_SEARCH_BRAVE_API_KEY="test-key",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
        ),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "brave_api"
    assert calls[0] == "http://localhost:8888/search"
    assert calls[1].startswith("https://api.search.brave.com")


def test_searxng_json_provider_parses_results():
    def requester(url, params, timeout, verify, headers):
        assert url == "http://localhost:8888/search"
        assert params["format"] == "json"
        return SimpleNamespace(
            status_code=200,
            text='{"results":[{"title":"Sigma Chi Demo","url":"https://example.org/chapter","content":"Official chapter site"}]}',
            json=lambda: {
                "results": [
                    {
                        "title": "Sigma Chi Demo",
                        "url": "https://example.org/chapter",
                        "content": "Official chapter site",
                    }
                ]
            },
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="searxng_json",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
        ),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "searxng_json"
    assert results[0].url == "https://example.org/chapter"


def test_auto_free_prefers_searxng_then_tavily_before_html_fallbacks():
    get_calls: list[str] = []
    post_calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        get_calls.append(url)
        raise requests.ConnectionError("searx unavailable")

    def post_requester(url, json, timeout, verify, headers):
        post_calls.append(url)
        if "api.tavily.com" in url:
            return SimpleNamespace(
                status_code=200,
                text='{"results":[{"title":"Sigma Chi Demo","url":"https://example.org/chapter","content":"Official chapter website"}]}',
                json=lambda: {
                    "results": [
                        {
                            "title": "Sigma Chi Demo",
                            "url": "https://example.org/chapter",
                            "content": "Official chapter website",
                        }
                    ]
                },
                raise_for_status=lambda: None,
            )
        raise AssertionError(f"Unexpected post URL: {url}")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto_free",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
            CRAWLER_SEARCH_TAVILY_API_KEY="test-key",
            CRAWLER_SEARCH_PROVIDER_ORDER_FREE="searxng_json,tavily_api,duckduckgo_html,bing_html",
        ),
        get_requester=requester,
        post_requester=post_requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "tavily_api"
    assert get_calls == ["http://localhost:8888/search"]
    assert post_calls == ["https://api.tavily.com/search"]


def test_auto_free_skips_unconfigured_api_providers_and_uses_duckduckgo_html():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "localhost:8888/search" in url:
            raise requests.ConnectionError("searx unavailable")
        if "duckduckgo" in url:
            return SimpleNamespace(
                status_code=200,
                text=(
                    "<html><body><table>"
                    "<tr><td><a href=\"//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.org%2Fchapter\">Sigma Chi at Demo University</a></td></tr>"
                    "</table></body></html>"
                ),
                raise_for_status=lambda: None,
            )
        raise AssertionError(f"Unexpected URL call: {url}")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto_free",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
            CRAWLER_SEARCH_PROVIDER_ORDER_FREE="searxng_json,tavily_api,serper_api,duckduckgo_html",
            CRAWLER_SEARCH_TAVILY_API_KEY="",
            CRAWLER_SEARCH_SERPER_API_KEY="",
        ),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "duckduckgo_html"
    assert calls[0] == "http://localhost:8888/search"
    assert calls[1].startswith("https://lite.duckduckgo.com")


def test_auto_free_falls_back_from_low_signal_searxng_results_to_duckduckgo():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "localhost:8888/search" in url:
            return SimpleNamespace(
                status_code=200,
                text='{"results":[{"title":"Directory Opus和Total Commander","url":"https://www.zhihu.com/question/22737842/answers/updated","content":"Directory Opus versus Total Commander"}]}',
                json=lambda: {
                    "results": [
                        {
                            "title": "Directory Opus和Total Commander",
                            "url": "https://www.zhihu.com/question/22737842/answers/updated",
                            "content": "Directory Opus versus Total Commander",
                        }
                    ]
                },
                raise_for_status=lambda: None,
            )
        if "duckduckgo" in url:
            return SimpleNamespace(
                status_code=200,
                text=(
                    "<html><body><table>"
                    "<tr><td><a href=\"//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.thetachi.org%2Fchapters\">Theta Chi Chapters</a></td></tr>"
                    "</table></body></html>"
                ),
                raise_for_status=lambda: None,
            )
        raise AssertionError(f"Unexpected URL call: {url}")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="auto_free",
            CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
            CRAWLER_SEARCH_PROVIDER_ORDER_FREE="searxng_json,duckduckgo_html",
        ),
        get_requester=requester,
    )

    results = client.search("theta chi official fraternity chapters")

    assert len(results) == 1
    assert results[0].provider == "duckduckgo_html"
    assert results[0].url == "https://www.thetachi.org/chapters"
    assert calls[0] == "http://localhost:8888/search"
    assert calls[1].startswith("https://lite.duckduckgo.com")


def test_search_client_caches_duplicate_queries():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        return SimpleNamespace(
            status_code=200,
            text="""<html><body><ol><li class="b_algo"><h2><a href="https://example.org/chapter">Sigma Chi at Demo University</a></h2><div class="b_caption"><p>Official chapter website</p></div></li></ol></body></html>""",
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="bing_html"),
        get_requester=requester,
    )

    first = client.search("sigma chi demo university website")
    second = client.search("sigma chi demo university website")

    assert first == second
    assert len(calls) == 1


def test_search_client_does_not_cache_empty_results_by_default():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        return SimpleNamespace(
            status_code=200,
            text="<html><body><ol></ol></body></html>",
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(database_url="postgresql://postgres:postgres@localhost:5433/fratfinder", CRAWLER_SEARCH_PROVIDER="bing_html"),
        get_requester=requester,
    )

    first = client.search("sigma chi demo university website")
    second = client.search("sigma chi demo university website")

    assert first == []
    assert second == []
    assert len(calls) == 6


def test_search_client_can_cache_empty_results_when_enabled():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        return SimpleNamespace(
            status_code=200,
            text="<html><body><ol></ol></body></html>",
            raise_for_status=lambda: None,
        )

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="bing_html",
            CRAWLER_SEARCH_CACHE_EMPTY_RESULTS=True,
        ),
        get_requester=requester,
    )

    first = client.search("sigma chi demo university website")
    second = client.search("sigma chi demo university website")

    assert first == []
    assert second == []
    assert len(calls) == 3


def test_search_client_opens_circuit_after_repeated_provider_failures():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        raise requests.ConnectionError("bing unavailable")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="bing_html",
            CRAWLER_SEARCH_CIRCUIT_BREAKER_FAILURES=2,
            CRAWLER_SEARCH_CIRCUIT_BREAKER_COOLDOWN_SECONDS=60,
        ),
        get_requester=requester,
    )

    with pytest.raises(requests.ConnectionError):
        client.search("sigma chi demo university website")
    with pytest.raises(requests.ConnectionError):
        client.search("sigma chi demo university website")
    with pytest.raises(SearchUnavailableError):
        client.search("sigma chi demo university website")

    # Calls fan out across free-provider fallbacks until all providers trip
    # their own circuit thresholds.
    assert len(calls) == 6


def test_bing_circuit_open_still_allows_free_provider_fallback():
    calls: list[str] = []

    def requester(url, params, timeout, verify, headers):
        calls.append(url)
        if "bing.com" in url:
            raise requests.ConnectionError("bing unavailable")
        if "duckduckgo" in url:
            return SimpleNamespace(
                status_code=200,
                text=(
                    "<html><body><table>"
                    "<tr><td><a href=\"//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.org%2Fchapter\">Sigma Chi at Demo University</a></td></tr>"
                    "</table></body></html>"
                ),
                raise_for_status=lambda: None,
            )
        raise AssertionError(f"Unexpected URL call: {url}")

    client = SearchClient(
        Settings(
            database_url="postgresql://postgres:postgres@localhost:5433/fratfinder",
            CRAWLER_SEARCH_PROVIDER="bing_html",
            CRAWLER_SEARCH_CIRCUIT_BREAKER_FAILURES=1,
            CRAWLER_SEARCH_CIRCUIT_BREAKER_COOLDOWN_SECONDS=60,
        ),
        get_requester=requester,
    )

    results = client.search("sigma chi demo university website")

    assert len(results) == 1
    assert results[0].provider == "duckduckgo_html"
    assert calls[0].startswith("https://www.bing.com")
    assert calls[1].startswith("https://lite.duckduckgo.com")
