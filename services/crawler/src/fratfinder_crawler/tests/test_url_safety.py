from __future__ import annotations

import pytest
import requests

from fratfinder_crawler.config import Settings
from fratfinder_crawler.security.url_safety import UrlSafetyError, safe_untrusted_fetch, safe_untrusted_get, validate_untrusted_url
from fratfinder_crawler.search import SearchClient


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost",
        "http://127.0.0.1",
        "http://0.0.0.0",
        "http://[::1]",
        "http://10.0.0.1",
        "http://172.16.0.1",
        "http://192.168.1.1",
        "http://169.254.169.254",
        "http://169.254.0.1",
        "http://224.0.0.1",
        "http://255.255.255.255",
        "http://[fc00::1]",
        "http://[fe80::1]",
    ],
)
def test_private_and_local_targets_are_blocked(url):
    with pytest.raises(UrlSafetyError, match="not allowed"):
        validate_untrusted_url(url)


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/file", "gopher://example.com", "data:text/plain,hello"])
def test_non_http_schemes_are_blocked(url):
    with pytest.raises(UrlSafetyError) as exc:
        validate_untrusted_url(url)
    assert exc.value.reason_code == "unsupported_scheme"


def test_embedded_credentials_are_blocked():
    with pytest.raises(UrlSafetyError) as exc:
        validate_untrusted_url("https://user:pass@example.com")
    assert exc.value.reason_code == "embedded_credentials_blocked"


def test_domain_resolving_to_private_ip_is_blocked():
    with pytest.raises(UrlSafetyError) as exc:
        validate_untrusted_url("https://example.test", resolver=lambda host: ["10.0.0.1"])
    assert exc.value.reason_code == "blocked_ip_range"


class FakeResponse:
    def __init__(self, *, url: str, status_code: int = 200, headers: dict[str, str] | None = None, body: bytes = b"<html></html>"):
        self.url = url
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "text/html"}
        self._body = body
        self.encoding = "utf-8"
        self.is_redirect = status_code in {301, 302, 303, 307, 308}
        self.text = body.decode("utf-8", errors="replace")

    def iter_content(self, chunk_size: int = 65536):
        yield self._body

    def json(self):
        import json

        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def test_redirect_to_private_ip_is_blocked():
    def requester(method, url, **kwargs):
        return FakeResponse(url=url, status_code=302, headers={"Location": "http://127.0.0.1/admin"})

    with pytest.raises(UrlSafetyError) as exc:
        safe_untrusted_get("https://public.example", resolver=lambda host: ["93.184.216.34"], requester=requester)
    assert exc.value.reason_code == "blocked_ip_range"


def test_oversized_response_is_blocked():
    def requester(method, url, **kwargs):
        return FakeResponse(url=url, body=b"x" * 10)

    with pytest.raises(UrlSafetyError) as exc:
        safe_untrusted_get("https://public.example", resolver=lambda host: ["93.184.216.34"], requester=requester, max_body_bytes=5)
    assert exc.value.reason_code == "response_too_large"


def test_disallowed_content_type_is_blocked():
    def requester(method, url, **kwargs):
        return FakeResponse(url=url, headers={"Content-Type": "application/octet-stream"})

    with pytest.raises(UrlSafetyError) as exc:
        safe_untrusted_get("https://public.example", resolver=lambda host: ["93.184.216.34"], requester=requester)
    assert exc.value.reason_code == "content_type_not_allowed"


def test_valid_public_https_html_fixture_is_allowed():
    def requester(method, url, **kwargs):
        return FakeResponse(url=url, body=b"<html><title>ok</title></html>")

    response = safe_untrusted_get("https://public.example", resolver=lambda host: ["93.184.216.34"], requester=requester)

    assert response.status_code == 200
    assert "ok" in response.text


def test_untrusted_fetch_strips_sensitive_headers():
    observed_headers: dict[str, str] = {}

    def requester(method, url, **kwargs):
        observed_headers.update(kwargs.get("headers") or {})
        return FakeResponse(url=url, body=b"<html><title>ok</title></html>")

    safe_untrusted_get(
        "https://public.example",
        resolver=lambda host: ["93.184.216.34"],
        requester=requester,
        headers={
            "Authorization": "Bearer internal-token",
            "Cookie": "session=secret",
            "X-Api-Key": "provider-key",
            "Accept-Language": "en-US",
        },
    )

    lowered = {key.lower() for key in observed_headers}
    assert "authorization" not in lowered
    assert "cookie" not in lowered
    assert "x-api-key" not in lowered
    assert observed_headers["Accept-Language"] == "en-US"


def test_default_untrusted_requester_disables_ambient_auth(monkeypatch):
    observed: dict[str, object] = {}

    class FakeSession:
        def __init__(self):
            self.trust_env = True

        def request(self, method, url, **kwargs):
            observed["trust_env"] = self.trust_env
            return FakeResponse(url=url, body=b"<html><title>ok</title></html>")

        def close(self):
            observed["closed"] = True

    monkeypatch.setattr("fratfinder_crawler.security.url_safety.requests.Session", FakeSession)

    safe_untrusted_fetch("https://public.example", resolver=lambda host: ["93.184.216.34"])

    assert observed["trust_env"] is False
    assert observed["closed"] is True


def test_local_searxng_untrusted_fetch_is_blocked():
    with pytest.raises(UrlSafetyError):
        validate_untrusted_url("http://localhost:8888/search?q=test&format=json")


def test_local_searxng_trusted_provider_path_is_allowed():
    calls: list[str] = []

    def requester(url, **kwargs):
        calls.append(url)
        return FakeResponse(url=url, headers={"Content-Type": "application/json"}, body=b'{"results":[{"url":"https://example.edu","title":"Example"}]}')

    settings = Settings(
        DATABASE_URL="postgresql://postgres:postgres@localhost:5432/fratfinder",
        CRAWLER_SEARCH_PROVIDER="searxng_json",
        CRAWLER_SEARCH_SEARXNG_BASE_URL="http://localhost:8888",
        CRAWLER_SEARCH_PROVIDER_ORDER_FREE="searxng_json",
        CRAWLER_SEARCH_MIN_REQUEST_INTERVAL_MS=0,
        CRAWLER_SEARCH_SEARXNG_MIN_INTERVAL_MS=0,
    )
    client = SearchClient(settings=settings, get_requester=requester)
    results = client.search("example")

    assert results
    assert calls and calls[0].startswith("http://localhost:8888")
