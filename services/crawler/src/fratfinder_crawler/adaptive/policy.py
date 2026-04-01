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

EXTRACTION_ACTIONS = {
    "extract_locator_api",
    "extract_script_json",
    "extract_table",
    "extract_repeated_block",
    "extract_stubs_only",
}

NAVIGATION_ACTIONS = {
    "expand_map_children",
    "expand_same_section_links",
    "expand_internal_links",
    "skip_page",
    "stop_branch",
    "review_branch",
}

RISKY_ACTIONS = {
    "expand_internal_links",
    "expand_map_children",
    "extract_locator_api",
}


class AdaptivePolicy:
    """Contextual bandit policy with split navigation/extraction heads.

    The policy keeps lightweight running reward summaries and augments
    heuristic scoring with learned priors plus explicit risk penalties.
    """

    def __init__(
        self,
        *,
        epsilon: float = 0.1,
        policy_version: str = "adaptive-v1",
        live_epsilon: float | None = None,
        train_epsilon: float | None = None,
        risk_timeout_weight: float = 0.75,
        risk_requeue_weight: float = 0.35,
    ) -> None:
        default_eps = max(0.0, min(epsilon, 1.0))
        self._live_epsilon = max(0.0, min(live_epsilon if live_epsilon is not None else default_eps, 1.0))
        self._train_epsilon = max(0.0, min(train_epsilon if train_epsilon is not None else default_eps, 1.0))
        self._policy_version = policy_version
        self._risk_timeout_weight = max(0.0, risk_timeout_weight)
        self._risk_requeue_weight = max(0.0, risk_requeue_weight)
        self._navigation_stats: dict[str, dict[str, float]] = {}
        self._extraction_stats: dict[str, dict[str, float]] = {}

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
        decisions.sort(key=lambda item: item.score, reverse=True)

        if mode == "adaptive_shadow" or not decisions:
            return decisions

        policy_mode = str(context.get("policy_mode") or "live").strip().lower()
        epsilon = self._train_epsilon if policy_mode == "train" else self._live_epsilon

        if random.random() < epsilon:
            selected = random.choice(decisions)
            selected.score_components["exploration_bonus"] = selected.score_components.get("exploration_bonus", 0.0) + 0.01
        else:
            selected = decisions[0]

        guardrail_flags = self._guardrail_flags(selected, decisions, policy_mode=policy_mode)
        if guardrail_flags:
            selected.score_components["guardrail_penalty"] = selected.score_components.get("guardrail_penalty", 0.0) - 0.05
            safer_choice = self._find_safer_choice(decisions)
            if safer_choice is not None:
                selected = safer_choice

        selected.score_components["policy_mode"] = 1.0 if policy_mode == "train" else 0.0
        selected.context = {**selected.context, "policyMode": policy_mode, "guardrailFlags": guardrail_flags}
        decisions.sort(key=lambda item: (item.action_type != selected.action_type, -item.score))
        return decisions

    def observe(self, action_type: str, reward_value: float) -> None:
        bucket = self._bucket_for_action(action_type)
        entry = bucket.setdefault(action_type, {"count": 0.0, "reward_sum": 0.0})
        entry["count"] += 1.0
        entry["reward_sum"] += reward_value

    def load_snapshot(self, payload: dict[str, Any] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        snapshot_version = str(payload.get("policyVersion") or "")
        if snapshot_version and snapshot_version != self._policy_version:
            return False

        # Backward compatibility: older payloads stored action stats at top level.
        if isinstance(payload.get("actions"), dict):
            restored = self._restore_action_map(payload.get("actions"))
            if not restored:
                return False
            self._extraction_stats = restored
            self._navigation_stats = {}
            return True

        nav = self._restore_action_map(payload.get("navigationActions"))
        ext = self._restore_action_map(payload.get("extractionActions"))
        if not nav and not ext:
            return False
        self._navigation_stats = nav
        self._extraction_stats = ext
        return True

    def snapshot(self) -> dict[str, Any]:
        return {
            "policyVersion": self._policy_version,
            "liveEpsilon": self._live_epsilon,
            "trainEpsilon": self._train_epsilon,
            "riskTimeoutWeight": self._risk_timeout_weight,
            "riskRequeueWeight": self._risk_requeue_weight,
            "navigationActions": self._snapshot_bucket(self._navigation_stats),
            "extractionActions": self._snapshot_bucket(self._extraction_stats),
        }

    def _snapshot_bucket(self, bucket: dict[str, dict[str, float]]) -> dict[str, dict[str, float | int]]:
        return {
            action: {
                "count": int(values["count"]),
                "avgReward": round(values["reward_sum"] / max(values["count"], 1.0), 4),
            }
            for action, values in bucket.items()
        }

    def _restore_action_map(self, payload: Any) -> dict[str, dict[str, float]]:
        if not isinstance(payload, dict):
            return {}
        restored: dict[str, dict[str, float]] = {}
        for action_type, entry in payload.items():
            if not isinstance(entry, dict):
                continue
            count = float(entry.get("count") or 0.0)
            avg_reward = float(entry.get("avgReward") or 0.0)
            if count <= 0:
                continue
            restored[str(action_type)] = {"count": count, "reward_sum": avg_reward * count}
        return restored

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
        timeout_risk = float(context.get("timeout_risk", 0.0))
        requeue_risk = float(context.get("requeue_risk", 0.0))

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

        action_stats = self._bucket_for_action(action).get(action, {"count": 0.0, "reward_sum": 0.0})
        observed_avg = action_stats["reward_sum"] / max(action_stats["count"], 1.0)
        observed_bonus = observed_avg * 0.2 if action_stats["count"] else 0.0

        risk_penalty = self._risk_penalty(action, timeout_risk=timeout_risk, requeue_risk=requeue_risk)

        score_components = {
            "prior": prior,
            "structural_bonus": structural_bonus,
            "template_bonus": template_bonus,
            "keyword_score": keyword_score * 0.15,
            "observed_bonus": observed_bonus,
            "risk_penalty": risk_penalty,
        }
        predicted_reward = round(prior + structural_bonus + template_bonus + observed_bonus + risk_penalty, 4)
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
                "timeoutRisk": timeout_risk,
                "requeueRisk": requeue_risk,
            },
        )

    def _risk_penalty(self, action: str, *, timeout_risk: float, requeue_risk: float) -> float:
        if action not in RISKY_ACTIONS:
            return 0.0
        timeout_term = min(max(timeout_risk, 0.0), 1.0) * self._risk_timeout_weight
        requeue_term = min(max(requeue_risk, 0.0), 1.0) * self._risk_requeue_weight
        return -(timeout_term + requeue_term)

    def _bucket_for_action(self, action: str) -> dict[str, dict[str, float]]:
        if action in EXTRACTION_ACTIONS:
            return self._extraction_stats
        if action in NAVIGATION_ACTIONS:
            return self._navigation_stats
        return self._navigation_stats

    def _guardrail_flags(self, selected: PolicyDecision, decisions: list[PolicyDecision], *, policy_mode: str) -> list[str]:
        flags: list[str] = []
        if policy_mode == "train":
            return flags
        timeout_risk = float(selected.context.get("timeoutRisk") or 0.0)
        requeue_risk = float(selected.context.get("requeueRisk") or 0.0)
        if selected.action_type in RISKY_ACTIONS and timeout_risk >= 0.8:
            flags.append("high_timeout_risk")
        if selected.action_type in RISKY_ACTIONS and requeue_risk >= 0.7:
            flags.append("high_requeue_risk")
        if selected.action_type == "expand_internal_links" and len(decisions) > 1 and decisions[1].score >= selected.score - 0.05:
            flags.append("prefer_safer_near_tie")
        return flags

    def _find_safer_choice(self, decisions: list[PolicyDecision]) -> PolicyDecision | None:
        for decision in decisions:
            if decision.action_type not in RISKY_ACTIONS:
                return decision
        return decisions[0] if decisions else None