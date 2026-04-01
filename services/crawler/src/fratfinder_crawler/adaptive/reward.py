from __future__ import annotations

from fratfinder_crawler.models import ExtractedChapter, RewardEvent


def score_reward(
    *,
    action_type: str,
    extracted: list[ExtractedChapter],
    links_added: int,
    timeout: bool = False,
    review_created: bool = False,
    valid_missing_count: int = 0,
    verified_website_count: int = 0,
    reward_stage: str = "immediate",
    discount_factor: float = 1.0,
    attributed_observation_id: int | None = None,
) -> RewardEvent:
    reward_components: dict[str, float] = {}
    if timeout:
        reward_components["timeout"] = -3.0
    if not extracted and valid_missing_count <= 0:
        reward_components["empty_page"] = -2.0

    chapters = len(extracted)
    if chapters:
        reward_components["chapters"] = chapters * 5.0
        contacts = 0
        for record in extracted:
            if record.website_url:
                contacts += 1
                reward_components["website"] = reward_components.get("website", 0.0) + 2.0
            if record.instagram_url:
                contacts += 1
                reward_components["instagram"] = reward_components.get("instagram", 0.0) + 2.0
            if record.contact_email:
                contacts += 1
                reward_components["email"] = reward_components.get("email", 0.0) + 3.0
        if contacts == 0:
            reward_components["chapter_without_contact"] = chapters * 1.0

    if verified_website_count > 0:
        # Positive incentive for finding chapter websites from trusted national sources.
        reward_components["trusted_website_seed"] = verified_website_count * 1.5

    if valid_missing_count > 0:
        # Conservative valid-missing detection should not be treated as model failure.
        reward_components["valid_missing"] = valid_missing_count * 0.35

    if links_added:
        reward_components["frontier_growth"] = min(links_added, 6) * 0.25
    if review_created:
        reward_components["review_penalty"] = -0.5

    reward_value = round(sum(reward_components.values()), 4)
    return RewardEvent(
        action_type=action_type,
        reward_value=reward_value,
        reward_components=reward_components,
        delayed=reward_stage == "delayed",
        reward_stage=reward_stage,
        attributed_observation_id=attributed_observation_id,
        discount_factor=discount_factor,
    )


def build_delayed_credit_events(
    *,
    ancestor_actions: list[tuple[int, str]],
    base_reward: float,
    gamma: float,
    attributed_observation_id: int | None,
    max_hops: int,
) -> list[RewardEvent]:
    events: list[RewardEvent] = []
    if base_reward <= 0:
        return events

    bounded_gamma = min(max(gamma, 0.0), 1.0)
    for hop, (_, action_type) in enumerate(ancestor_actions[: max(1, max_hops)], start=1):
        discount = bounded_gamma ** hop
        value = round(base_reward * discount, 4)
        if value <= 0:
            continue
        events.append(
            RewardEvent(
                action_type=action_type,
                reward_value=value,
                reward_components={"path_credit": value, "hop": float(hop)},
                delayed=True,
                reward_stage="delayed",
                attributed_observation_id=attributed_observation_id,
                discount_factor=round(discount, 4),
            )
        )
    return events


def score_terminal_reward(
    *,
    status: str,
    stop_reason: str | None,
    queue_efficiency: float,
) -> RewardEvent:
    reward_components: dict[str, float] = {
        "queue_efficiency": round(max(min(queue_efficiency, 1.0), -1.0) * 2.0, 4)
    }
    if status == "succeeded":
        reward_components["run_complete"] = 1.0
    elif status == "failed":
        reward_components["run_failed"] = -1.5
    if stop_reason in {"frontier_empty", "seed_frontier_empty"}:
        reward_components["clean_stop"] = 0.25
    if stop_reason == "page_budget_exhausted":
        reward_components["budget_exhausted"] = -0.4

    value = round(sum(reward_components.values()), 4)
    return RewardEvent(
        action_type="terminal_reward",
        reward_value=value,
        reward_components=reward_components,
        delayed=False,
        reward_stage="terminal",
        attributed_observation_id=None,
        discount_factor=1.0,
    )