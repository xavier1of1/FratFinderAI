from dataclasses import dataclass

from fratfinder_crawler.adaptive.frontier import canonicalize_url, score_frontier_item
from fratfinder_crawler.adaptive.policy import AdaptivePolicy
from fratfinder_crawler.adaptive.stop_conditions import evaluate_stop_conditions
from fratfinder_crawler.adaptive.template_memory import compute_template_signature
from fratfinder_crawler.models import PageAnalysis, TemplateProfile
from fratfinder_crawler.orchestration.adaptive_graph import _sanitize_json_value, _to_serializable


def _analysis(**overrides):
    payload = {
        "title": "Chapter Directory",
        "headings": ["Chapters"],
        "table_count": 1,
        "repeated_block_count": 2,
        "link_count": 18,
        "has_json_ld": False,
        "has_script_json": False,
        "has_map_widget": False,
        "has_pagination": False,
        "probable_page_role": "directory",
        "text_sample": "chapter directory",
    }
    payload.update(overrides)
    return PageAnalysis(**payload)


def test_frontier_scoring_prefers_chapter_directory_path_over_alumni_page():
    analysis = _analysis()
    chapter_score, _ = score_frontier_item(
        "https://sigmachi.org/chapters/",
        anchor_text="Find a chapter",
        depth=1,
        source_url="https://sigmachi.org/chapters/",
        page_analysis=analysis,
    )
    alumni_score, _ = score_frontier_item(
        "https://sigmachi.org/alumni/",
        anchor_text="Alumni",
        depth=1,
        source_url="https://sigmachi.org/chapters/",
        page_analysis=analysis,
    )
    assert chapter_score > alumni_score


def test_template_signature_is_stable_for_same_shape():
    analysis = _analysis(repeated_block_count=3)
    left = compute_template_signature("https://example.org/chapters?page=1", analysis)
    right = compute_template_signature("https://example.org/chapters?page=2", analysis)
    assert left == right


def test_adaptive_policy_prefers_template_best_action():
    policy = AdaptivePolicy(epsilon=0.0)
    profile = TemplateProfile(
        template_signature="example|directory",
        host_family="example.org",
        best_action_family="extract_table",
        best_extraction_family="extract_table",
        visit_count=5,
        chapter_yield=3.0,
        contact_yield=1.5,
    )
    decisions = policy.choose_action(
        ["extract_repeated_block", "extract_table", "expand_internal_links"],
        context={
            "page_type": "static_directory",
            "probable_page_role": "directory",
            "has_map_widget": False,
            "has_script_json": False,
            "table_count": 2,
            "repeated_block_count": 1,
            "keyword_score": 2.0,
        },
        template_profile=profile,
        mode="adaptive_assisted",
    )
    assert decisions[0].action_type == "extract_table"


def test_stop_conditions_fire_on_budget_exhaustion():
    should_stop, reason = evaluate_stop_conditions(
        budget_state={
            "pages_processed": 40,
            "max_pages": 40,
            "empty_streak": 0,
            "max_empty_streak": 5,
            "low_yield_streak": 0,
            "saturation_threshold": 4,
            "min_score": 0.1,
        },
        frontier_remaining=3,
        current_score=1.0,
    )
    assert should_stop is True
    assert reason == "page_budget_exhausted"


def test_adaptive_policy_can_resume_from_snapshot():
    policy = AdaptivePolicy(epsilon=0.0)
    loaded = policy.load_snapshot(
        {
            "policyVersion": policy.policy_version,
            "actions": {
                "extract_table": {"count": 10, "avgReward": 3.5},
            },
        }
    )
    assert loaded is True
    decisions = policy.choose_action(
        ["extract_table", "extract_repeated_block"],
        context={
            "page_type": "static_directory",
            "probable_page_role": "directory",
            "has_map_widget": False,
            "has_script_json": False,
            "table_count": 1,
            "repeated_block_count": 0,
            "keyword_score": 0.0,
        },
        template_profile=None,
        mode="adaptive_assisted",
    )
    assert decisions[0].action_type == "extract_table"


def test_template_signature_coarsens_similar_paths():
    analysis = _analysis()
    left = compute_template_signature("https://example.org/chapters/california/alpha", analysis)
    right = compute_template_signature("https://example.org/chapters/oregon/beta", analysis)
    assert left == right


def test_canonicalize_url_drops_tracker_query_params():
    normalized = canonicalize_url(
        "https://example.org/chapters/?utm_source=abc&fbclid=1&page=2&gclid=xyz"
    )
    assert normalized == "https://example.org/chapters?page=2"


def test_adaptive_shadow_policy_returns_score_sorted_candidates():
    policy = AdaptivePolicy(epsilon=0.0)
    decisions = policy.choose_action(
        ["review_branch", "extract_table", "extract_repeated_block"],
        context={
            "page_type": "static_directory",
            "probable_page_role": "directory",
            "has_map_widget": False,
            "has_script_json": False,
            "table_count": 2,
            "repeated_block_count": 1,
            "keyword_score": 0.0,
        },
        template_profile=None,
        mode="adaptive_shadow",
    )
    scores = [decision.score for decision in decisions]
    assert scores == sorted(scores, reverse=True)

def test_stop_conditions_fire_on_high_yield_saturation():
    should_stop, reason = evaluate_stop_conditions(
        budget_state={
            "pages_processed": 3,
            "max_pages": 40,
            "records_seen": 95,
            "high_yield_record_threshold": 80,
            "min_pages_for_high_yield_stop": 2,
            "empty_streak": 0,
            "max_empty_streak": 5,
            "low_yield_streak": 1,
            "saturation_threshold": 4,
            "min_score": 0.1,
        },
        frontier_remaining=6,
        current_score=0.5,
    )
    assert should_stop is True
    assert reason == "high_yield_saturated"


@dataclass
class _SerializableFixture:
    name: str
    payload: dict


def test_adaptive_graph_sanitize_json_value_removes_null_bytes_and_normalizes_tuples():
    payload = {
        "title": "Alpha\x00Beta",
        "items": ("one\x00", {"nested": "two\x00"}),
    }

    sanitized = _sanitize_json_value(payload)

    assert sanitized == {"title": "AlphaBeta", "items": ["one", {"nested": "two"}]}


def test_adaptive_graph_to_serializable_handles_dataclasses_and_rejects_scalars():
    fixture = _SerializableFixture(name="Alpha\x00Beta", payload={"items": ("x\x00", "y")})

    serialized = _to_serializable(fixture)

    assert serialized == {"name": "AlphaBeta", "payload": {"items": ["x", "y"]}}
    assert _to_serializable("not-a-structured-payload") is None
