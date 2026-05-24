from __future__ import annotations

import ipaddress
import logging
import socket
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping
from urllib.parse import urljoin, urlparse

import requests


DEFAULT_ALLOWED_CONTENT_TYPES = (
    "text/html",
    "application/xhtml+xml",
    "application/json",
    "text/plain",
    "application/xml",
    "text/xml",
    "application/rss+xml",
    "application/atom+xml",
)
DEFAULT_MAX_BODY_BYTES = 2_000_000
DEFAULT_MAX_REDIRECTS = 3

_LOGGER = logging.getLogger(__name__)
_SENSITIVE_UNTRUSTED_HEADER_NAMES = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "x-auth-token",
    "x-access-token",
    "x-session-token",
    "x-csrf-token",
}


class UrlSafetyError(requests.RequestException, ValueError):
    def __init__(self, reason_code: str, message: str, *, url: str | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.url = url
        _LOGGER.warning("untrusted_url_denied reason=%s url=%s", reason_code, url or "")


@dataclass(frozen=True)
class SafeFetchResponse:
    url: str
    status_code: int
    headers: Mapping[str, str]
    text: str
    content: bytes

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} response for {self.url}")


def _is_blocked_ip(ip_address: str) -> bool:
    parsed = ipaddress.ip_address(ip_address)
    return bool(
        parsed.is_loopback
        or parsed.is_private
        or parsed.is_link_local
        or parsed.is_multicast
        or parsed.is_reserved
        or parsed.is_unspecified
    )


def _resolve_hostname(hostname: str, resolver: Callable[[str], Iterable[str]] | None = None) -> list[str]:
    try:
        ipaddress.ip_address(hostname)
        return [hostname]
    except ValueError:
        pass
    if resolver is not None:
        return [str(item) for item in resolver(hostname)]
    try:
        return sorted({item[4][0] for item in socket.getaddrinfo(hostname, None)})
    except socket.gaierror as exc:
        raise UrlSafetyError("hostname_resolution_failed", f"Could not resolve hostname `{hostname}`.") from exc


def validate_untrusted_url(url: str, *, resolver: Callable[[str], Iterable[str]] | None = None) -> None:
    parsed = urlparse(str(url or "").strip())
    if not parsed.scheme:
        raise UrlSafetyError("invalid_url", "URL must include a scheme and host.", url=url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise UrlSafetyError("unsupported_scheme", f"Unsupported URL scheme `{parsed.scheme}`.", url=url)
    if not parsed.netloc:
        raise UrlSafetyError("invalid_url", "URL must include a scheme and host.", url=url)
    if parsed.username or parsed.password:
        raise UrlSafetyError("embedded_credentials_blocked", "Embedded URL credentials are not allowed.", url=url)
    if not parsed.hostname:
        raise UrlSafetyError("invalid_url", "URL must include a hostname.", url=url)

    resolved_ips = _resolve_hostname(parsed.hostname, resolver=resolver)
    if not resolved_ips:
        raise UrlSafetyError("hostname_resolution_failed", f"Hostname `{parsed.hostname}` resolved no addresses.", url=url)
    for resolved_ip in resolved_ips:
        try:
            parsed_ip = ipaddress.ip_address(resolved_ip)
        except ValueError as exc:
            raise UrlSafetyError("hostname_resolution_failed", f"Resolved value `{resolved_ip}` is not an IP address.", url=url) from exc
        if _is_blocked_ip(str(parsed_ip)):
            raise UrlSafetyError("blocked_ip_range", f"Resolved IP `{resolved_ip}` is not allowed.", url=url)


def _allowed_content_type(content_type: str, allowed_content_types: Iterable[str]) -> bool:
    actual = (content_type or "").split(";", 1)[0].strip().lower()
    if not actual:
        return False
    return actual in {item.strip().lower() for item in allowed_content_types}


def _sanitize_untrusted_headers(
    headers: Mapping[str, str] | None,
    *,
    allowed_content_types: Iterable[str],
) -> dict[str, str]:
    sanitized = {
        "User-Agent": "FratFinderAI-SafeFetcher/1.0",
        "Accept": ",".join(allowed_content_types),
    }
    for name, value in (headers or {}).items():
        header_name = str(name or "").strip()
        if not header_name or "\r" in header_name or "\n" in header_name:
            continue
        if header_name.lower() in _SENSITIVE_UNTRUSTED_HEADER_NAMES:
            continue
        header_value = str(value or "")
        if "\r" in header_value or "\n" in header_value:
            continue
        sanitized[header_name] = header_value
    return sanitized


def _default_requester(method: str, url: str, **kwargs) -> requests.Response:
    # Do not inherit ambient proxy, netrc, or other process-level auth config for
    # crawler-discovered URLs. Trusted providers use their own client path.
    session = requests.Session()
    session.trust_env = False
    try:
        return session.request(method, url, **kwargs)
    finally:
        session.close()


def safe_untrusted_fetch(
    url: str,
    *,
    method: str = "GET",
    timeout: float = 20.0,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    max_redirects: int = DEFAULT_MAX_REDIRECTS,
    allowed_content_types: Iterable[str] = DEFAULT_ALLOWED_CONTENT_TYPES,
    headers: Mapping[str, str] | None = None,
    verify: bool = True,
    resolver: Callable[[str], Iterable[str]] | None = None,
    requester: Callable[..., requests.Response] | None = None,
) -> SafeFetchResponse:
    """Fetch a crawler-discovered URL with application-layer SSRF controls."""

    current_url = str(url or "").strip()
    request_fn = requester or _default_requester
    redirects_followed = 0
    method_upper = method.upper()
    while True:
        validate_untrusted_url(current_url, resolver=resolver)
        try:
            response = request_fn(
                method_upper,
                current_url,
                timeout=timeout,
                allow_redirects=False,
                stream=True,
                headers=_sanitize_untrusted_headers(headers, allowed_content_types=allowed_content_types),
                verify=verify,
            )
        except requests.Timeout as exc:
            raise UrlSafetyError("request_timeout", f"Timed out fetching `{current_url}`.", url=current_url) from exc
        except requests.RequestException as exc:
            raise UrlSafetyError("request_error", f"Request failed for `{current_url}`.", url=current_url) from exc

        if response.is_redirect or response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("Location")
            if not location:
                raise UrlSafetyError("redirect_blocked", "Redirect response did not include a Location header.", url=current_url)
            redirects_followed += 1
            if redirects_followed > max_redirects:
                raise UrlSafetyError("too_many_redirects", "Redirect limit exceeded.", url=current_url)
            current_url = urljoin(current_url, location)
            continue

        if method_upper == "HEAD":
            return SafeFetchResponse(
                url=str(response.url or current_url),
                status_code=int(response.status_code),
                headers=response.headers,
                text="",
                content=b"",
            )

        content_type = response.headers.get("Content-Type") or response.headers.get("content-type") or ""
        if not _allowed_content_type(content_type, allowed_content_types):
            raise UrlSafetyError("content_type_not_allowed", f"Content type `{content_type or 'missing'}` is not allowed.", url=current_url)

        body = bytearray()
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            body.extend(chunk)
            if len(body) > max_body_bytes:
                raise UrlSafetyError("response_too_large", f"Response exceeded {max_body_bytes} bytes.", url=current_url)
        content = bytes(body)
        encoding = response.encoding or "utf-8"
        return SafeFetchResponse(
            url=str(response.url or current_url),
            status_code=int(response.status_code),
            headers=response.headers,
            text=content.decode(encoding, errors="replace"),
            content=content,
        )


def safe_untrusted_get(url: str, **kwargs) -> SafeFetchResponse:
    return safe_untrusted_fetch(url, method="GET", **kwargs)


def safe_untrusted_head(url: str, **kwargs) -> SafeFetchResponse:
    return safe_untrusted_fetch(url, method="HEAD", **kwargs)
