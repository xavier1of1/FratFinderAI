from fratfinder_crawler.models import ExtractedChapter, SourceRecord
from fratfinder_crawler.normalization.normalizer import normalize_record



def _source() -> SourceRecord:
    return SourceRecord(
        id="source-1",
        fraternity_id="frat-1",
        fraternity_slug="beta-theta-pi",
        source_slug="beta-theta-pi-main",
        source_type="html_directory",
        parser_key="directory_v1",
        base_url="https://example.org",
        list_path="/chapters",
        metadata={},
    )



def test_normalizer_builds_slug_and_queues_missing_optional_fields():
    extracted = ExtractedChapter(
        name="Gamma Alpha",
        university_name="University of Michigan",
        city="Ann Arbor",
        state="MI",
        website_url=None,
        source_url="https://example.org/chapters",
    )

    normalized, provenance = normalize_record(_source(), extracted)

    assert normalized.slug == "gamma-alpha-university-of-michigan"
    assert "find_website" in normalized.missing_optional_fields
    assert "find_email" in normalized.missing_optional_fields
    assert "find_instagram" in normalized.missing_optional_fields
    assert normalized.field_states["website_url"] == "missing"
    assert any(item.field_name == "name" for item in provenance)



def test_normalizer_does_not_queue_find_instagram_when_high_confidence_instagram_exists():
    extracted = ExtractedChapter(
        name="Delta Beta",
        university_name="Ohio State University",
        instagram_url="https://instagram.com/deltabeta",
        source_url="https://example.org/chapters",
        source_confidence=0.95,
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert "find_instagram" not in normalized.missing_optional_fields
    assert normalized.field_states["instagram_url"] == "found"



def test_normalizer_queues_find_instagram_when_instagram_missing():
    extracted = ExtractedChapter(
        name="Epsilon Zeta",
        university_name="University of Georgia",
        source_url="https://example.org/chapters",
        source_confidence=0.95,
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert "find_instagram" in normalized.missing_optional_fields
    assert normalized.field_states["instagram_url"] == "missing"



def test_normalizer_queues_verify_website_when_website_is_present_but_low_confidence():
    extracted = ExtractedChapter(
        name="Alpha Phi",
        university_name="Cornell University",
        website_url="https://chapters.example.org/alpha-phi",
        source_url="https://example.org/chapters",
        source_confidence=0.7,
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert "verify_website" in normalized.missing_optional_fields
    assert "find_website" not in normalized.missing_optional_fields
    assert normalized.field_states["website_url"] == "low_confidence"
def test_normalizer_does_not_queue_verify_website_when_website_is_high_confidence():
    extracted = ExtractedChapter(
        name="Alpha Nu",
        university_name="University of Texas-Austin",
        website_url="https://chapters.example.org/alpha-nu",
        source_url="https://example.org/chapters",
        source_confidence=0.95,
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert "verify_website" not in normalized.missing_optional_fields
    assert "find_website" not in normalized.missing_optional_fields
    assert normalized.field_states["website_url"] == "found"

