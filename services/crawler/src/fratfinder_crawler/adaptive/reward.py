from __future__ import annotations

from fratfinder_crawler.models import ExtractedChapter, RewardEvent


def score_reward(
    *,
    action_type: str,
    extracted: list[ExtractedChapter],
    links_added: int,
    timeout: bool = False,
    review_created: bool = False,
) -> RewardEvent:
    reward_components: dict[str, float] = {}
    if timeout:
        reward_components["timeout"] = -3.0
    if not extracted:
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
    if links_added:
        reward_components["frontier_growth"] = min(links_added, 6) * 0.25
    if review_created:
        reward_components["review_penalty"] = -0.5
    reward_value = round(sum(reward_components.values()), 4)
    return RewardEvent(action_type=action_type, reward_value=reward_value, reward_components=reward_components, delayed=False)
