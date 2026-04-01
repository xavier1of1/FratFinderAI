from __future__ import annotations

import random
from typing import Any

from fratfinder_crawler.models import PolicyDecision, TemplateProfile

BASE_ACTION_PRIORS = {
    "extract_locator_api": 2.5,
    "extract_script_json": 2.1,
    "extract_table": 1.6,
    "extract_repeated_block": 1.4,
    "extract_stubs_only": 1.0,
    "expand_map_children": 0.9,
    "expand_same_section_links": 0.8,
    "expand_internal_links": 0.4,
    "review_branch": -0.5,
    "skip_page": -0.6,
    "stop_branch": -1.0,
}


class AdaptivePolicy:
    """Heuristic + lightweight contextual-bandit wrapper.

    River is the long-term estimator target, but we keep the boundary here so
    the adaptive runtime works immediately even when River is not installed yet.
    """

    def __init__(self, *, epsilon: float = 0.1, policy_version: str = "adaptive-v1") -> None:
        self._epsilon = max(0.0, min(epsilon, 1.0))
        self._policy_version = policy_version
        self._action_stats: dict[str, dict[str, float]] = {}

    @property
    def policy_version(self) -> str:
        return self._policy_version

    def choose_action(
        self,
        actions: list[str],
        *,
        context: dict[str, Any],
        template_profile: TemplateProfile | None = None,
        mode: str = "adaptive_shadow",
    ) -> list[PolicyDecision]:
        decisions = [self._score_action(action, context=context, template_profile=template_profile) for action in actions]
        if mode == "adaptive_shadow":
            # Keep shadow behavior deterministic and inspectable by score order.
            decisions.sort(key=lambda item: item.score, reverse=True)
            return decisions
        if mode in {"adaptive_assisted", "adaptive_primary"} and decisions:
            if random.random() < self._epsilon:
                selected = random.choice(decisions)
                selected.score_components["exploration_bonus"] = selected.score_components.get("exploration_bonus", 0.0) + 0.01
            else:
                selected = max(decisions, key=lambda item: item.score)
            decisions.sort(key=lambda item: (item.action_type != selected.action_type, -item.score))
            return decisions
        return decisions

    def observe(self, action_type: str, reward_value: float) -> None:
        bucket = self._action_stats.setdefault(action_type, {"count": 0.0, "reward_sum": 0.0})
        bucket["count"] += 1.0
        bucket["reward_sum"] += reward_value

    def load_snapshot(self, payload: dict[str, Any] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        snapshot_version = str(payload.get("policyVersion") or "")
        if snapshot_version and snapshot_version != self._policy_version:
            return False
        raw_actions = payload.get("actions")
        if not isinstance(raw_actions, dict):
            return False
        restored: dict[str, dict[str, float]] = {}
        for action_type, entry in raw_actions.items():
            if not isinstance(entry, dict):
                continue
            count = float(entry.get("count") or 0.0)
            avg_reward = float(entry.get("avgReward") or 0.0)
            if count <= 0:
                continue
            restored[str(action_type)] = {"count": count, "reward_sum": avg_reward * count}
        if not restored:
            return False
        self._action_stats = restored
        return True

    def snapshot(self) -> dict[str, Any]:
        return {
            "policyVersion": self._policy_version,
            "epsilon": self._epsilon,
            "actions": {
                action: {
                    "count": int(values["count"]),
                    "avgReward": round(values["reward_sum"] / max(values["count"], 1.0), 4),
                }
                for action, values in self._action_stats.items()
            },
        }

    def _score_action(
        self,
        action: str,
        *,
        context: dict[str, Any],
        template_profile: TemplateProfile | None,
    ) -> PolicyDecision:
        prior = BASE_ACTION_PRIORS.get(action, 0.0)
        page_type = str(context.get("page_type") or "")
        probable_role = str(context.get("probable_page_role") or "")
        has_map_widget = bool(context.get("has_map_widget", False))
        has_script_json = bool(context.get("has_script_json", False))
        table_count = int(context.get("table_count", 0))
        repeated_block_count = int(context.get("repeated_block_count", 0))
        keyword_score = float(context.get("keyword_score", 0.0))

        structural_bonus = 0.0
        if action == "extract_locator_api" and has_map_widget:
            structural_bonus += 2.0
        if action == "extract_script_json" and has_script_json:
            structural_bonus += 1.7
        if action == "extract_table" and table_count > 0:
            structural_bonus += min(table_count, 3) * 0.7
        if action == "extract_repeated_block" and repeated_block_count > 0:
            structural_bonus += min(repeated_block_count, 4) * 0.5
        if action in {"expand_internal_links", "expand_same_section_links", "expand_map_children"} and probable_role in {"directory", "index"}:
            structural_bonus += 0.5
        if action == "extract_stubs_only" and page_type in {"static_directory", "locator_map"}:
            structural_bonus += 0.6

        template_bonus = 0.0
        if template_profile is not None:
            if template_profile.best_action_family == action:
                template_bonus += 1.4
            if template_profile.best_extraction_family == action:
                template_bonus += 1.2
            template_bonus += min(template_profile.chapter_yield, 3.0) * 0.2
            template_bonus += min(template_profile.contact_yield, 3.0) * 0.15
            template_bonus -= min(template_profile.empty_rate, 1.0) * 0.8
            template_bonus -= min(template_profile.timeout_rate, 1.0) * 0.8

        action_stats = self._action_stats.get(action, {"count": 0.0, "reward_sum": 0.0})
        observed_avg = action_stats["reward_sum"] / max(action_stats["count"], 1.0)
        observed_bonus = observed_avg * 0.2 if action_stats["count"] else 0.0

        score_components = {
            "prior": prior,
            "structural_bonus": structural_bonus,
            "template_bonus": template_bonus,
            "keyword_score": keyword_score * 0.15,
            "observed_bonus": observed_bonus,
        }
        predicted_reward = round(prior + structural_bonus + template_bonus + observed_bonus, 4)
        return PolicyDecision(
            action_type=action,
            score=round(sum(score_components.values()), 4),
            score_components=score_components,
            predicted_reward=predicted_reward,
            context={
                "pageType": page_type,
                "probablePageRole": probable_role,
                "tableCount": table_count,
                "repeatedBlockCount": repeated_block_count,
            },
        )
