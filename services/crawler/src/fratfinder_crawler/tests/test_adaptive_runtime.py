from fratfinder_crawler.adaptive.frontier import score_frontier_item
from fratfinder_crawler.adaptive.policy import AdaptivePolicy
from fratfinder_crawler.adaptive.stop_conditions import evaluate_stop_conditions
from fratfinder_crawler.adaptive.template_memory import compute_template_signature
from fratfinder_crawler.models import PageAnalysis, TemplateProfile


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
