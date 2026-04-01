from fratfinder_crawler.adaptive.policy import AdaptivePolicy
from fratfinder_crawler.adaptive.reward import build_delayed_credit_events, score_reward
from fratfinder_crawler.adaptive.template_memory import compute_structural_template_signature
from fratfinder_crawler.models import PageAnalysis


def _analysis(**overrides) -> PageAnalysis:
    payload = {
        "title": "Chapter Directory",
        "headings": ["Find a Chapter"],
        "table_count": 1,
        "repeated_block_count": 2,
        "link_count": 14,
        "has_json_ld": False,
        "has_script_json": False,
        "has_map_widget": True,
        "has_pagination": False,
        "probable_page_role": "directory",
        "text_sample": "national chapter map",
    }
    payload.update(overrides)
    return PageAnalysis(**payload)


def test_score_reward_grants_valid_missing_and_trusted_website_credit():
    reward = score_reward(
        action_type="extract_stubs_only",
        extracted=[],
        links_added=0,
        valid_missing_count=2,
        verified_website_count=3,
    )
    assert reward.reward_components["valid_missing"] == 0.7
    assert reward.reward_components["trusted_website_seed"] == 4.5
    assert "empty_page" not in reward.reward_components
    assert reward.reward_value == 5.2


def test_build_delayed_credit_events_applies_discount_and_hop_cap():
    events = build_delayed_credit_events(
        ancestor_actions=[
            (11, "expand_map_children"),
            (7, "expand_same_section_links"),
            (3, "extract_stubs_only"),
        ],
        base_reward=8.0,
        gamma=0.85,
        attributed_observation_id=55,
        max_hops=2,
    )
    assert len(events) == 2
    assert events[0].action_type == "expand_map_children"
    assert events[0].reward_stage == "delayed"
    assert events[0].discount_factor == 0.85
    assert events[0].reward_value == 6.8
    assert events[1].discount_factor == 0.7225
    assert events[1].reward_value == 5.78
    assert all(event.attributed_observation_id == 55 for event in events)


def test_live_policy_guardrail_switches_off_high_risk_choice():
    policy = AdaptivePolicy(live_epsilon=0.0, train_epsilon=0.0)
    decisions = policy.choose_action(
        ["extract_locator_api", "extract_script_json"],
        context={
            "policy_mode": "live",
            "page_type": "locator_map",
            "probable_page_role": "directory",
            "has_map_widget": True,
            "has_script_json": False,
            "table_count": 0,
            "repeated_block_count": 0,
            "keyword_score": 0.0,
            "timeout_risk": 0.95,
            "requeue_risk": 0.8,
        },
        template_profile=None,
        mode="adaptive_assisted",
    )
    selected = decisions[0]
    assert selected.action_type == "extract_script_json"
    assert "high_timeout_risk" in selected.context["guardrailFlags"]
    assert "high_requeue_risk" in selected.context["guardrailFlags"]


def test_train_policy_can_keep_risky_selection_without_live_guardrail_flags():
    policy = AdaptivePolicy(live_epsilon=0.0, train_epsilon=0.0)
    decisions = policy.choose_action(
        ["extract_locator_api", "extract_script_json"],
        context={
            "policy_mode": "train",
            "page_type": "locator_map",
            "probable_page_role": "directory",
            "has_map_widget": True,
            "has_script_json": False,
            "table_count": 0,
            "repeated_block_count": 0,
            "keyword_score": 0.5,
            "timeout_risk": 0.95,
            "requeue_risk": 0.8,
        },
        template_profile=None,
        mode="adaptive_assisted",
    )
    selected = decisions[0]
    assert selected.action_type == "extract_locator_api"
    assert selected.context["policyMode"] == "train"
    assert selected.context["guardrailFlags"] == []


def test_structural_signature_generalizes_state_route_shape_across_hosts():
    analysis = _analysis()
    left = compute_structural_template_signature("https://alpha.example.org/chapters/california/theta", analysis)
    right = compute_structural_template_signature("https://beta.example.com/chapters/oregon/gamma", analysis)
    assert left == right
