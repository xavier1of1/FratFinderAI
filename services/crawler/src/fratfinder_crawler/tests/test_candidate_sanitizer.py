from fratfinder_crawler.candidate_sanitizer import (
    CandidateKind,
    classify_candidate_kind,
    sanitize_as_email,
    sanitize_as_instagram,
    sanitize_as_website,
    sanitize_candidate,
)


def test_sanitize_website_accepts_http_and_rejects_mailto():
    assert sanitize_as_website("https://example.org/chapter") == "https://example.org/chapter"
    assert sanitize_as_website("mailto:admin@example.org") is None


def test_sanitize_email_normalizes_mailto():
    assert sanitize_as_email("mailto:Admin@Example.ORG") == "admin@example.org"
    assert sanitize_as_email("not-an-email") is None


def test_sanitize_instagram_normalizes_handle_and_url():
    assert sanitize_as_instagram("@sigmachiuchicago") == "https://www.instagram.com/sigmachiuchicago"
    assert sanitize_as_instagram("https://www.instagram.com/sigmachiuchicago/") == "https://www.instagram.com/sigmachiuchicago"
    assert sanitize_as_instagram("https://www.instagram.com/tel") is None


def test_sanitize_candidate_reroutes_website_kind_mismatch():
    result = sanitize_candidate("mailto:hello@example.org", expected=CandidateKind.WEBSITE)
    assert result is not None
    assert result.kind == CandidateKind.EMAIL
    assert result.value == "hello@example.org"


def test_classify_candidate_kind_detects_instagram():
    assert classify_candidate_kind("https://instagram.com/sigmachiuchicago") == CandidateKind.INSTAGRAM
