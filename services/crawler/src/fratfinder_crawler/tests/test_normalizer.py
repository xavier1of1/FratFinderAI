import pytest

from fratfinder_crawler.models import AmbiguousRecordError, ExtractedChapter, SourceRecord
from fratfinder_crawler.normalization.normalizer import classify_chapter_validity, normalize_record


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


def test_normalizer_does_not_queue_missing_contact_jobs_when_identity_is_weak():
    extracted = ExtractedChapter(
        name="Epsilon Zeta",
        university_name=None,
        source_url="https://example.org/chapters",
        source_confidence=0.6,
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert "find_website" not in normalized.missing_optional_fields
    assert "find_email" not in normalized.missing_optional_fields
    assert "find_instagram" not in normalized.missing_optional_fields


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


def test_normalizer_rejects_navigation_placeholder_name():
    extracted = ExtractedChapter(
        name="Find a Chapter",
        university_name=None,
        source_url="https://example.org/chapters",
        source_confidence=0.95,
    )

    with pytest.raises(AmbiguousRecordError, match="navigation or placeholder"):
        normalize_record(_source(), extracted)


def test_normalizer_rejects_visit_page_placeholder_pattern():
    extracted = ExtractedChapter(
        name="Visit",
        university_name="Page Active Beta St. Louis University",
        source_url="https://example.org/chapters",
        source_confidence=0.95,
    )

    with pytest.raises(AmbiguousRecordError, match="navigation or placeholder"):
        normalize_record(_source(), extracted)


def test_normalizer_routes_mailto_website_into_email_field():
    extracted = ExtractedChapter(
        name="Psi Chapter",
        university_name="University of Virginia",
        website_url="mailto:admin@chapter.org",
        source_url="https://example.org/chapters",
        source_confidence=0.92,
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert normalized.website_url is None
    assert normalized.contact_email == "admin@chapter.org"
    assert normalized.field_states["website_url"] == "missing"
    assert normalized.field_states["contact_email"] == "found"

def test_normalizer_marks_valid_missing_when_conservative_evidence_exists():
    extracted = ExtractedChapter(
        name="Gamma Chapter",
        university_name="Inactive Chapter",
        source_url="https://example.org/chapters",
        source_confidence=0.95,
        source_snippet="This chapter is currently suspended and no longer active.",
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert normalized.field_states["website_url"] == "valid_missing"
    assert normalized.field_states["instagram_url"] == "valid_missing"
    assert normalized.field_states["contact_email"] == "valid_missing"
    assert "find_website" not in normalized.missing_optional_fields
    assert "find_instagram" not in normalized.missing_optional_fields
    assert "find_email" not in normalized.missing_optional_fields


def test_normalizer_does_not_mark_valid_missing_when_any_contact_is_present():
    extracted = ExtractedChapter(
        name="Gamma Chapter",
        university_name="Inactive Chapter",
        website_url="https://gamma.example.edu",
        source_url="https://example.org/chapters",
        source_confidence=0.95,
        source_snippet="This chapter is currently suspended and no longer active.",
    )

    normalized, _ = normalize_record(_source(), extracted)

    assert normalized.field_states["website_url"] == "found"
    assert normalized.field_states["instagram_url"] == "missing"
    assert normalized.field_states["contact_email"] == "missing"
    assert "find_instagram" in normalized.missing_optional_fields
    assert "find_email" in normalized.missing_optional_fields


def test_chapter_validity_blocks_school_division_rows():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="School of Medicine",
            university_name="University of Example",
            source_url="https://example.edu/greek-life",
            source_confidence=0.95,
        ),
        source_class="institutional",
    )

    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason == "school_division_or_department"


def test_chapter_validity_blocks_award_rows():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="The Most Outstanding Chapter Award",
            university_name="2008",
            source_url="https://adg.example.org/awards",
            source_confidence=0.95,
        ),
        source_class="national",
    )

    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason in {"award_or_honor_row", "year_or_percentage_as_identity"}


def test_chapter_validity_blocks_wikipedia_ranking_rows():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="Best Career Services",
            university_name="#16",
            source_url="https://en.wikipedia.org/wiki/University_of_Example",
            source_confidence=0.95,
        ),
        source_class="national",
    )

    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason == "ranking_or_report_row"


def test_chapter_validity_blocks_wikipedia_sports_or_category_pairs():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="Baseball",
            university_name="Basketball",
            source_url="https://en.wikipedia.org/wiki/University_of_Example",
            source_confidence=0.95,
            source_snippet="Baseball Basketball",
        ),
        source_class="national",
    )

    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason == "ranking_or_report_row"


def test_chapter_validity_blocks_history_timeline_rows_with_year_ranges():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="Elizabeth Davis 2014-present",
            university_name=None,
            source_url="https://sae.example.org/history",
            source_confidence=0.95,
        ),
        source_class="national",
    )

    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason == "history_or_timeline_row"


def test_chapter_validity_blocks_history_rows_when_year_range_is_in_university_field():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="Charles Manly",
            university_name="1881-1897",
            source_url="https://en.wikipedia.org/wiki/Sigma_Alpha_Epsilon",
            source_confidence=0.95,
            source_snippet="charles-manly-1881-1897",
        ),
        source_class="national",
    )

    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason == "history_or_timeline_row"


def test_chapter_validity_blocks_person_name_rows_when_snippet_has_year_range():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="John Edwin Johns",
            university_name="Example University",
            source_url="https://sae.example.org/history",
            source_confidence=0.95,
            source_snippet="john-edwin-johns-1976-1994",
        ),
        source_class="national",
    )

    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason == "history_or_timeline_row"


def test_chapter_validity_marks_wider_web_candidates_provisional():
    decision = classify_chapter_validity(
        ExtractedChapter(
            name="Gamma Alpha",
            university_name="University of Michigan",
            source_url="https://independent.example.org/gamma-alpha",
            source_confidence=0.92,
        ),
        source_class="wider_web",
    )

    assert decision.validity_class == "provisional_candidate"
    assert decision.repair_reason == "broader_web_gated"


def test_normalizer_blocks_contact_queue_for_non_canonical_validity():
    extracted = ExtractedChapter(
        name="Gamma Alpha",
        university_name="University of Michigan",
        source_url="https://example.org/chapters",
        source_confidence=0.95,
    )

    normalized, _ = normalize_record(_source(), extracted, validity_class="provisional_candidate")

    assert "find_website" not in normalized.missing_optional_fields
    assert "find_email" not in normalized.missing_optional_fields
    assert "find_instagram" not in normalized.missing_optional_fields
