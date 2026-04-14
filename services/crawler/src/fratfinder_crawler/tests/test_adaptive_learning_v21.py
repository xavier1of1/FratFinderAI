from types import SimpleNamespace

from fratfinder_crawler.adaptive.policy import AdaptivePolicy
from fratfinder_crawler.adaptive.reward import build_delayed_credit_events, score_reward, score_terminal_reward
from fratfinder_crawler.adaptive.template_memory import compute_structural_template_signature
from fratfinder_crawler.models import FrontierItem, PageAnalysis
from fratfinder_crawler.orchestration.adaptive_graph import AdaptiveCrawlOrchestrator


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


def test_build_delayed_credit_events_propagates_negative_terminal_penalty():
    events = build_delayed_credit_events(
        ancestor_actions=[
            (11, "expand_map_children"),
            (7, "extract_stubs_only"),
        ],
        base_reward=-2.5,
        gamma=0.8,
        attributed_observation_id=91,
        max_hops=2,
    )

    assert len(events) == 2
    assert events[0].reward_value == -2.0
    assert events[1].reward_value == -1.6
    assert events[0].discount_factor == 0.8
    assert events[1].discount_factor == 0.64


def test_terminal_reward_includes_business_outcomes_not_just_queue_efficiency():
    reward = score_terminal_reward(
        status="succeeded",
        stop_reason="frontier_empty",
        queue_efficiency=0.4,
        records_upserted=3,
        canonical_created=2,
        provisional_created=1,
        inline_enriched=2,
        blocked_invalid=3,
        blocked_repairable=1,
        review_items_created=1,
        source_invalidity_saturated=False,
    )

    assert reward.reward_components["queue_efficiency"] == 0.8
    assert reward.reward_components["canonical_validated"] == 1.5
    assert reward.reward_components["inline_contact_progress"] == 0.8
    assert reward.reward_components["invalid_blocked"] == 0.24
    assert reward.reward_components["review_load_penalty"] == -0.08
    assert reward.reward_value > 0


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


def test_enrichment_policy_prefers_parse_supporting_page_when_chapter_support_exists():
    policy = AdaptivePolicy(live_epsilon=0.0, train_epsilon=0.0)
    decisions = policy.choose_action(
        ["parse_supporting_page", "search_web", "defer"],
        context={
            "policy_mode": "live",
            "field_type": "find_email",
            "supporting_page_present": True,
            "supporting_page_scope": "chapter_site",
            "provider_window_healthy": True,
            "provider_window_degraded": False,
            "website_prerequisite_unmet": False,
            "prior_query_count": 0,
            "identity_complete": True,
            "has_candidate_website": True,
            "has_target_value": False,
            "needs_authoritative_validation": False,
            "timeout_risk": 0.1,
            "requeue_risk": 0.1,
        },
        template_profile=None,
        mode="adaptive_assisted",
    )

    assert decisions[0].action_type == "parse_supporting_page"


def test_enrichment_policy_prefers_defer_when_provider_is_degraded_and_prereqs_missing():
    policy = AdaptivePolicy(live_epsilon=0.0, train_epsilon=0.0)
    decisions = policy.choose_action(
        ["search_web", "search_social", "defer"],
        context={
            "policy_mode": "live",
            "field_type": "find_instagram",
            "supporting_page_present": False,
            "supporting_page_scope": "",
            "provider_window_healthy": False,
            "provider_window_degraded": True,
            "website_prerequisite_unmet": True,
            "prior_query_count": 2,
            "identity_complete": False,
            "has_candidate_website": False,
            "has_target_value": False,
            "needs_authoritative_validation": False,
            "timeout_risk": 1.0,
            "requeue_risk": 0.8,
        },
        template_profile=None,
        mode="adaptive_assisted",
    )

    assert decisions[0].action_type == "defer"


def test_structural_signature_generalizes_state_route_shape_across_hosts():
    analysis = _analysis()
    left = compute_structural_template_signature("https://alpha.example.org/chapters/california/theta", analysis)
    right = compute_structural_template_signature("https://beta.example.com/chapters/oregon/gamma", analysis)
    assert left == right


def test_frontier_filter_blocks_irrelevant_same_host_pages_after_chapter_yield():
    orchestrator = AdaptiveCrawlOrchestrator.__new__(AdaptiveCrawlOrchestrator)
    orchestrator._settings = SimpleNamespace(crawler_frontier_max_pages_per_template=8)
    state = {
        "source": SimpleNamespace(list_url="https://tke.org/join-tke/find-a-chapter/"),
        "current_frontier_item": FrontierItem(
            id=None,
            url="https://tke.org/join-tke/find-a-chapter/",
            canonical_url="https://tke.org/join-tke/find-a-chapter",
            parent_url=None,
            depth=0,
            anchor_text="seed",
            discovered_from="seed",
        ),
        "page_analysis": _analysis(),
        "selected_action": "extract_script_json",
        "chapter_stubs": [object()],
        "extracted_from_current": [],
    }

    assert orchestrator._should_queue_frontier_link(state, {"url": "https://tke.org/careers/", "anchor_text": "Careers"}) is False
    assert orchestrator._should_queue_frontier_link(
        state,
        {"url": "https://tke.org/join-tke/find-a-chapter/alpha-beta/", "anchor_text": "Alpha Beta"},
    ) is True


def test_frontier_budget_shrinks_when_page_already_emitted_chapter_signals():
    orchestrator = AdaptiveCrawlOrchestrator.__new__(AdaptiveCrawlOrchestrator)
    orchestrator._settings = SimpleNamespace(crawler_frontier_max_pages_per_template=8)

    assert orchestrator._generic_frontier_link_budget({"chapter_stubs": [], "extracted_from_current": []}) == 8
    assert orchestrator._generic_frontier_link_budget({"chapter_stubs": [object()], "extracted_from_current": []}) == 3


def test_delayed_reward_seed_counts_chapter_yield_not_just_contacts():
    orchestrator = AdaptiveCrawlOrchestrator.__new__(AdaptiveCrawlOrchestrator)
    seed = orchestrator._delayed_reward_seed(
        {
            "extracted_from_current": [
                SimpleNamespace(contact_email=None, instagram_url=None),
                SimpleNamespace(contact_email="alpha@example.edu", instagram_url=None),
            ],
            "verified_website_count_current": 1,
            "valid_missing_count_current": 1,
        }
    )

    assert seed == 3.1


def test_chapter_search_decision_rejects_wider_web_record_without_institution_signal():
    orchestrator = AdaptiveCrawlOrchestrator.__new__(AdaptiveCrawlOrchestrator)
    decision = orchestrator._chapter_search_decide_record(
        record=SimpleNamespace(
            name="Alpha Chapter",
            university_name=None,
            website_url="https://alphachapter.example/",
            source_url="https://alphachapter.example/",
        ),
        source_class="wider_web",
    )

    assert decision.decision == "reject"
    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason == "identity_semantically_incomplete"


def test_chapter_search_decision_routes_award_rows_to_invalid_non_chapter():
    orchestrator = AdaptiveCrawlOrchestrator.__new__(AdaptiveCrawlOrchestrator)
    decision = orchestrator._chapter_search_decide_record(
        record=SimpleNamespace(
            name="The Most Outstanding Chapter Award",
            university_name="2008",
            website_url=None,
            source_url="https://adg.example.org/awards",
            source_confidence=0.95,
            source_snippet=None,
        ),
        source_class="national",
    )

    assert decision.decision == "reject"
    assert decision.validity_class == "invalid_non_chapter"
    assert decision.invalid_reason in {"award_or_honor_row", "year_or_percentage_as_identity"}


def test_chapter_search_decision_marks_incomplete_national_identity_repairable():
    orchestrator = AdaptiveCrawlOrchestrator.__new__(AdaptiveCrawlOrchestrator)
    decision = orchestrator._chapter_search_decide_record(
        record=SimpleNamespace(
            name="Mississippi",
            university_name=None,
            website_url=None,
            source_url="https://phigam.org/about/overview/our-chapters/",
            source_confidence=0.9,
            source_snippet=None,
        ),
        source_class="national",
    )

    assert decision.decision == "repair"
    assert decision.validity_class == "repairable_candidate"
    assert decision.repair_reason == "identity_semantically_incomplete"


def test_chapter_search_follow_stats_track_skipped_chapter_sites():
    orchestrator = AdaptiveCrawlOrchestrator.__new__(AdaptiveCrawlOrchestrator)
    merged = orchestrator._merge_chapter_search_follow_stats(
        {},
        {
            "followed_by_target_type": {"national_detail": 2, "institutional_page": 1},
            "skipped_by_target_type": {"chapter_owned_site": 4},
        },
    )

    assert merged["nationalTargetsFollowed"] == 2
    assert merged["institutionalTargetsFollowed"] == 1
    assert merged["chapterOwnedTargetsSkipped"] == 4
