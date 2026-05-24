# SSRF-Safe Outbound URL Policy

## Purpose

FratFinderAI crawls public fraternity, school, chapter, and social/contact pages. This policy prevents crawler-discovered URLs from reaching local services, private networks, cloud metadata endpoints, or unsafe schemes.

## Threat/Risk Addressed

Without application-layer URL controls, an attacker-controlled source URL or redirect could cause the crawler to request internal services such as `localhost`, RFC1918 addresses, or `169.254.169.254`.

## Implementation Summary

Untrusted crawler-discovered URLs are fetched through `fratfinder_crawler.security.url_safety`.

The wrapper:

- Allows only `http` and `https`.
- Rejects embedded credentials.
- Resolves DNS before requests.
- Blocks loopback, private, link-local, multicast, reserved, unspecified, and metadata IP ranges.
- Disables automatic redirects.
- Revalidates every redirect target.
- Caps response size.
- Enforces allowed content types.
- Strips sensitive outbound headers such as cookies, bearer tokens, proxy auth, and API-key style headers.
- Avoids ambient process auth/proxy inheritance for untrusted URLs.
- Uses stable deny reason codes.

## Trusted vs Untrusted Fetch Paths

Untrusted paths include source pages, national directory pages, chapter detail pages, status/campus pages, field-job result pages, and social backfill pages.

Trusted provider paths include configured search providers such as local SearXNG, Serper, Tavily, and other API clients. These are explicit operator-controlled dependencies and are not routed through the untrusted fetch wrapper.

## Configuration

Defaults:

```text
CRAWLER_HTTP_MAX_BODY_BYTES=2000000
CRAWLER_HTTP_MAX_REDIRECTS=3
CRAWLER_HTTP_ALLOWED_CONTENT_TYPES=text/html,application/xhtml+xml,application/json,text/plain,application/xml,text/xml,application/rss+xml,application/atom+xml
```

## Deny Reason Codes

| Reason | Meaning |
| --- | --- |
| `invalid_url` | URL is malformed or missing scheme/host. |
| `unsupported_scheme` | Scheme is not HTTP or HTTPS. |
| `embedded_credentials_blocked` | URL contains username/password credentials. |
| `hostname_resolution_failed` | Hostname cannot be resolved safely. |
| `blocked_ip_range` | Host resolves to a blocked local/private/reserved range. |
| `redirect_blocked` | Redirect is malformed or missing a target. |
| `too_many_redirects` | Redirect limit was exceeded. |
| `content_type_not_allowed` | Response content type is outside the allowlist. |
| `response_too_large` | Response exceeded the configured byte cap. |
| `request_timeout` | Request timed out. |
| `request_error` | Non-timeout request failure after URL validation. |

## Local SearXNG Exception

`http://localhost:8888` and similar local SearXNG endpoints are intentionally blocked when treated as untrusted crawler targets. They remain usable only through the trusted search-provider client, where the endpoint is explicitly configured by the operator.

## Validation Commands

```powershell
python -m pytest services/crawler/src/fratfinder_crawler/tests -k "url_safety or ssrf"
python -m pytest services/crawler/src/fratfinder_crawler/tests --cov=fratfinder_crawler --cov-report=term-missing --cov-fail-under=70
python -m pytest tests/integration -m integration
pnpm lint
pnpm typecheck
```

## Expected Logs

Denied URLs should carry one of the stable reason codes above. Provider outages and SSRF denials should not be converted into false positive contact writes or unsafe canonical updates.

## Known Limitations

This is an application-layer guard. It reduces SSRF risk but does not replace container/network egress policies, firewall rules, cloud metadata protections, or DNS pinning at the infrastructure layer.

## Future Hardening Steps

- Add network-level egress allowlists for production workers.
- Add DNS pinning or custom transport support for stricter DNS rebinding protection.
- Add structured denied-URL telemetry to the operator console.
