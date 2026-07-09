# Phase 2 SSRF-Safe Outbound Fetch Validation Report

Date: 2026-05-23

Workspace: `<repo root>`

## Executive Result

Phase 2 is complete locally. FratFinderAI now has a centralized SSRF-safe fetch layer for crawler-discovered URLs, automated call-site coverage to prevent raw untrusted `requests` usage from returning, fixture-backed tests for the required blocked targets and redirect/content/body controls, and documentation explaining trusted versus untrusted fetch paths.

The implementation preserves trusted configured provider behavior. Local SearXNG remains available through the trusted search provider client and is blocked when treated as an untrusted crawler URL.

## Requirement Fulfillment Matrix

| ID | Requirement | Fulfillment |
| --- | --- | --- |
| SSRF-1 | Create central module `services/crawler/src/fratfinder_crawler/security/url_safety.py`. | Implemented in `fratfinder_crawler.security.url_safety`. |
| SSRF-2 | Expose a single safe untrusted fetch wrapper used for crawler-discovered URLs. | `safe_untrusted_fetch`, `safe_untrusted_get`, and `safe_untrusted_head` centralize untrusted fetch behavior. |
| SSRF-3 | Allow only HTTP and HTTPS. | `validate_untrusted_url` rejects non-HTTP(S) schemes with `unsupported_scheme`. |
| SSRF-4 | Reject `file`, `ftp`, `gopher`, `data`, `dict`, and all other schemes. | Unit tests cover `file`, `ftp`, `gopher`, and `data`; implementation rejects any scheme outside `http`/`https`. |
| SSRF-5 | Reject malformed URLs and URLs without hostname/netloc. | `invalid_url` is raised for missing scheme, netloc, or hostname. |
| SSRF-6 | Reject embedded credentials. | `embedded_credentials_blocked` is raised when username/password are present. |
| SSRF-7 | Resolve DNS before request. | Hostname resolution happens before every request and redirect target fetch. |
| SSRF-8 | Validate all resolved IPv4 and IPv6 addresses. | Every resolved address is parsed and checked with `ipaddress`. |
| SSRF-9 | Block loopback, private, link-local, multicast, reserved, unspecified, and cloud metadata ranges. | Blocked through `ipaddress` flags and tested against required IPv4/IPv6 targets. |
| SSRF-10 | Specifically block `169.254.169.254`. | Covered by link-local blocking and explicit unit test. |
| SSRF-11 | Disable automatic redirects. | Requests are issued with `allow_redirects=False`. |
| SSRF-12 | Manually follow redirects up to `CRAWLER_HTTP_MAX_REDIRECTS`. | Redirect loop follows manually and enforces `max_redirects`, default `3`. |
| SSRF-13 | Re-run full URL/IP validation on every redirect target. | Redirect target is assigned to `current_url` and re-enters `validate_untrusted_url`. |
| SSRF-14 | Cap response body size using streaming reads. | Uses `iter_content` and raises `response_too_large` once the byte cap is exceeded. |
| SSRF-15 | Enforce allowed content types. | Content type is checked before body consumption. |
| SSRF-16 | Set connect/read timeouts. | Safe wrapper passes configured timeout into the request call. |
| SSRF-17 | Do not send internal cookies, auth headers, DB credentials, provider API keys, or session data. | Sensitive auth/cookie/API-key style headers are stripped, and default untrusted requests disable ambient process auth/proxy inheritance with `Session.trust_env = False`. |
| SSRF-18 | Log denied URLs with stable reason codes. | `UrlSafetyError` logs `untrusted_url_denied reason=<code> url=<url>`. |
| SSRF-19 | Keep trusted configured provider endpoints separate from untrusted crawler URLs. | Raw provider clients remain explicit exceptions; untrusted crawler fetches use the safe wrapper. |
| SSRF-20 | Keep local SearXNG working only through trusted-provider path. | Unit tests prove local SearXNG is blocked through untrusted fetch and allowed through `SearchClient`. |
| SSRF-21 | Document application-layer limitation. | Documented in `docs/security/ssrf-url-safety.md`. |
| SSRF-22 | Add SSRF docs. | Added `docs/security/ssrf-url-safety.md`. |

## Crawler Call-Site Coverage

The following untrusted crawler paths call `safe_untrusted_*`:

```text
services/crawler/src/fratfinder_crawler\discovery.py:684:    response = safe_untrusted_get(
services/crawler/src/fratfinder_crawler\field_jobs.py:633:        return safe_untrusted_head(
services/crawler/src/fratfinder_crawler\field_jobs.py:640:        return safe_untrusted_get(
services/crawler/src/fratfinder_crawler\http\client.py:80:        return safe_untrusted_get(
services/crawler/src/fratfinder_crawler\pipeline.py:5445:        response = safe_untrusted_get(
services/crawler/src/fratfinder_crawler\social\bulk_backfill_instagram.py:197:            response = safe_untrusted_get(url, timeout=10)
services/crawler/src/fratfinder_crawler\status\page_fetcher.py:20:            else safe_untrusted_get(url, timeout=timeout)
```

The raw-request scan shows only approved exceptions:

```text
services/crawler/src/fratfinder_crawler\http\client.py:38:        self._session = _SafeBrowserSession(
services/crawler/src/fratfinder_crawler\search\client.py:126:        self._session: requests.Session | None = None
services/crawler/src/fratfinder_crawler\search\client.py:128:            session = requests.Session()
services/crawler/src/fratfinder_crawler\search\client.py:136:            self._get_requester = get_requester or requests.get
services/crawler/src/fratfinder_crawler\search\client.py:137:            self._post_requester = post_requester or requests.post
services/crawler/src/fratfinder_crawler\security\url_safety.py:145:    session = requests.Session()
services/crawler/src/fratfinder_crawler\search\searxng_health.py:204:    request_fn = requester or requests.get
```

These are covered by `test_ssrf_callsite_coverage.py`. The approved exceptions are the safe wrapper itself, the trusted search provider client, trusted SearXNG health diagnostics, and the safe browser session wrapper that delegates into `safe_untrusted_get`.

## Required Blocked Target Coverage

The unit tests cover:

```text
localhost
127.0.0.1
0.0.0.0
[::1]
10.0.0.1
172.16.0.1
192.168.1.1
169.254.169.254
169.254.0.1
224.0.0.1
255.255.255.255
fc00::1
fe80::1
domain resolving to private IP
public URL redirecting to private IP
file:///etc/passwd
oversized response
disallowed content type
local SearXNG through untrusted fetch
```

The tests also prove a valid public HTTPS HTML fixture is allowed and local SearXNG still works through the trusted provider path.

## Real Validation Logs

### Focused SSRF Tests

Command:

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests -k "url_safety or ssrf"
```

Result:

```text
............................                                             [100%]
28 passed, 561 deselected in 3.36s
```

### Security-Focused Tests

Command:

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests/test_security_sca_policy.py services/crawler/src/fratfinder_crawler/tests/test_url_safety.py services/crawler/src/fratfinder_crawler/tests/test_ssrf_callsite_coverage.py
```

Result:

```text
....................................                                     [100%]
36 passed in 1.10s
```

### Full Crawler Suite With Coverage Gate

Command:

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70
```

Result:

```text
589 passed, 1 warning in 68.88s (0:01:08)
Required test coverage of 70% reached. Total coverage: 70.84%
```

### Integration Tests

Command:

```powershell
python -m pytest tests/integration -m integration
```

Result:

```text
tests\integration\test_local_demo_flow.py .                              [ 50%]
tests\integration\test_status_first_queue_flow.py .                      [100%]
2 passed, 1 warning in 74.69s (0:01:14)
```

### Web Lint

Command:

```powershell
pnpm.cmd lint
```

Result:

```text
> frat-finder-ai@3.0.4 lint <repo root>
> pnpm --filter @fratfinder/contracts lint && pnpm --filter @fratfinder/web lint

> @fratfinder/contracts@3.0.4 lint <repo root>\packages\contracts
> tsc --noEmit

> @fratfinder/web@3.0.4 lint <repo root>\apps\web
> tsc --noEmit --incremental false -p tsconfig.typecheck.json
```

### Web Typecheck

Command:

```powershell
pnpm.cmd typecheck
```

Result:

```text
> frat-finder-ai@3.0.4 typecheck <repo root>
> pnpm --filter @fratfinder/contracts typecheck && pnpm --filter @fratfinder/web typecheck

> @fratfinder/contracts@3.0.4 typecheck <repo root>\packages\contracts
> tsc --noEmit

> @fratfinder/web@3.0.4 typecheck <repo root>\apps\web
> tsc --noEmit --incremental false -p tsconfig.typecheck.json
```

## Deny Reason Codes Implemented

```text
invalid_url
unsupported_scheme
embedded_credentials_blocked
hostname_resolution_failed
blocked_ip_range
redirect_blocked
too_many_redirects
content_type_not_allowed
response_too_large
request_timeout
request_error
```

`request_error` is an additional stable reason for non-timeout request failures after URL validation.

## Configuration Verified

The required defaults are present in config, `.env.example`, and docs:

```text
CRAWLER_HTTP_MAX_BODY_BYTES=2000000
CRAWLER_HTTP_MAX_REDIRECTS=3
CRAWLER_HTTP_ALLOWED_CONTENT_TYPES=text/html,application/xhtml+xml,application/json,text/plain,application/xml,text/xml,application/rss+xml,application/atom+xml
```

## Documented Exceptions

The following raw request paths are intentional:

- `security/url_safety.py`: the safe wrapper implementation itself.
- `search/client.py`: trusted configured provider client, including SearXNG/API providers.
- `search/searxng_health.py`: trusted SearXNG operational diagnostics.
- tests and fixtures.

No untrusted crawler path may add direct raw `requests.get`, `requests.head`, `requests.post`, `requests.request`, or `requests.Session` usage without failing the static call-site coverage test.

## Conclusion

Phase 2 satisfies the implementation, testing, static coverage, integration, documentation, and preservation requirements. The SSRF guard blocks the required dangerous destinations and schemes, revalidates redirects, caps body and content type, preserves trusted provider behavior, and has automated tests that protect the boundary from regression.
