from fratfinder_crawler.config import Settings
from fratfinder_crawler.http.client import HttpClient


class DummyResponse:
    def __init__(self, text: str = "ok") -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_http_client_sends_browser_headers_and_origin_referer(monkeypatch):
    settings = Settings(DATABASE_URL="postgresql://example:test@localhost:5432/fratfinder")
    client = HttpClient(settings)
    captured: dict[str, object] = {}

    def fake_get(url: str, *, headers=None, timeout=None, verify=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["timeout"] = timeout
        captured["verify"] = verify
        return DummyResponse("payload")

    monkeypatch.setattr(client._session, "get", fake_get)

    result = client.get("https://dke.org/chapter-experience/chapters/")

    assert result == "payload"
    assert client._session.headers["Accept"].startswith("text/html")
    assert client._session.headers["Accept-Language"] == "en-US,en;q=0.9"
    assert client._session.headers["Upgrade-Insecure-Requests"] == "1"
    assert client._session.headers["User-Agent"].startswith("Mozilla/5.0")
    assert captured["headers"] == {"Referer": "https://dke.org/"}
    assert captured["timeout"] == settings.crawler_http_timeout_seconds
    assert captured["verify"] == settings.crawler_http_verify_ssl


def test_http_client_replaces_legacy_bot_user_agent_with_browser_profile():
    settings = Settings(
        DATABASE_URL="postgresql://example:test@localhost:5432/fratfinder",
        CRAWLER_HTTP_USER_AGENT="FratFinderAI/1.0 (+https://example.com/fratfinder)",
    )

    client = HttpClient(settings)

    assert client._session.headers["User-Agent"].startswith("Mozilla/5.0")


def test_http_client_preserves_explicit_custom_browserish_user_agent():
    settings = Settings(
        DATABASE_URL="postgresql://example:test@localhost:5432/fratfinder",
        CRAWLER_HTTP_USER_AGENT="CustomBrowser/9.9",
    )

    client = HttpClient(settings)

    assert client._session.headers["User-Agent"] == "CustomBrowser/9.9"
