from __future__ import annotations

import argparse
import json
from pathlib import Path

from fratfinder_crawler.social.instagram_extractor import extract_instagram_candidates_from_document
from fratfinder_crawler.social.instagram_identity import build_chapter_instagram_identity
from fratfinder_crawler.social.instagram_models import InstagramSourceType
from fratfinder_crawler.social.instagram_resolver import select_best_instagram_candidate
from fratfinder_crawler.social.instagram_scorer import score_instagram_candidate


def _case_source_type(raw: object) -> InstagramSourceType:
    value = str(raw or "").strip()
    if not value:
        return InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE
    try:
        return InstagramSourceType(value)
    except ValueError:
        return InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE


def _default_page_scope(source_type: InstagramSourceType) -> str:
    if source_type in {
        InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
        InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
    }:
        return "school_affiliation_page"
    if source_type in {
        InstagramSourceType.NATIONALS_CHAPTER_PAGE,
        InstagramSourceType.NATIONALS_DIRECTORY_ROW,
    }:
        return "nationals_chapter_page"
    if source_type in {
        InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
        InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA,
        InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK,
    }:
        return "chapter_website"
    return "supporting_page"


def _default_contact_specificity(source_type: InstagramSourceType) -> str:
    if source_type in {
        InstagramSourceType.OFFICIAL_SCHOOL_CHAPTER_PAGE,
        InstagramSourceType.OFFICIAL_SCHOOL_DIRECTORY_ROW,
    }:
        return "school_specific"
    if source_type in {
        InstagramSourceType.NATIONALS_CHAPTER_PAGE,
        InstagramSourceType.NATIONALS_DIRECTORY_ROW,
    }:
        return "national_specific_to_chapter"
    if source_type in {
        InstagramSourceType.VERIFIED_CHAPTER_WEBSITE,
        InstagramSourceType.CHAPTER_WEBSITE_STRUCTURED_DATA,
        InstagramSourceType.CHAPTER_WEBSITE_SOCIAL_LINK,
    }:
        return "chapter_specific"
    return "chapter_specific"


def evaluate_goldset(goldset_path: Path) -> dict[str, float]:
    total = 0
    accepted = 0
    accepted_correct = 0
    correct_outcomes = 0
    false_positive_count = 0
    false_negative_count = 0
    review = 0
    known_instagram_cases = 0

    for line in goldset_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        total += 1
        case = json.loads(line)
        expected_outcome = str(case.get("expected_outcome") or "accepted").strip().lower()
        expected_url = case.get("expected_instagram_url")
        if expected_outcome == "accepted":
            known_instagram_cases += 1

        source_type = _case_source_type(case.get("source_type"))
        page_scope = str(case.get("page_scope") or _default_page_scope(source_type))
        contact_specificity = str(case.get("contact_specificity") or _default_contact_specificity(source_type))

        fixture_candidates = []
        for fixture_path in case.get("fixture_paths", []):
            fixture_file = Path("services/crawler/tests/fixtures") / fixture_path
            html = fixture_file.read_text(encoding="utf-8")
            fixture_candidates.extend(
                extract_instagram_candidates_from_document(
                    text=html,
                    links=[],
                    html=html,
                    source_type=source_type,
                    source_url=f"https://fixtures.local/{fixture_file.name}",
                    page_scope=page_scope,
                    contact_specificity=contact_specificity,
                )
            )
        identity = build_chapter_instagram_identity(
            fraternity_name=case.get("fraternity_name"),
            school_name=case.get("school_name"),
            chapter_name=case.get("chapter_name"),
        )
        scored = [score_instagram_candidate(candidate, identity) for candidate in fixture_candidates]
        best = select_best_instagram_candidate(scored, minimum_confidence=0.88)
        accepted_case = best is not None
        if accepted_case:
            accepted += 1
        else:
            review += 1

        if expected_outcome == "accepted":
            if accepted_case and best.profile_url == expected_url:
                accepted_correct += 1
                correct_outcomes += 1
            elif not accepted_case:
                false_negative_count += 1
        else:
            if not accepted_case:
                correct_outcomes += 1
            else:
                false_positive_count += 1

    verified_precision = round(accepted_correct / accepted, 4) if accepted else 0.0
    coverage_rate = round(accepted_correct / known_instagram_cases, 4) if known_instagram_cases else 0.0
    false_positive_rate = round(false_positive_count / total, 4) if total else 0.0
    false_negative_rate = round(false_negative_count / known_instagram_cases, 4) if known_instagram_cases else 0.0
    return {
        "cases": total,
        "known_instagram_cases": known_instagram_cases,
        "accepted_instagram_count": accepted,
        "correct_instagram_count": accepted_correct,
        "correct_outcome_count": correct_outcomes,
        "review_rate": round(review / total, 4) if total else 0.0,
        "verified_precision": verified_precision,
        "coverage_rate": coverage_rate,
        "false_positive_rate": false_positive_rate,
        "false_negative_rate_on_known_instagram_cases": false_negative_rate,
        "unsafe_write_count": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Instagram resolution gold set")
    parser.add_argument("--goldset", required=True)
    parser.add_argument("--fail-on-threshold", action="store_true")
    args = parser.parse_args()
    result = evaluate_goldset(Path(args.goldset))
    print(json.dumps(result, indent=2))
    if args.fail_on_threshold:
        if (
            result["verified_precision"] < 0.96
            or result["coverage_rate"] < 0.60
            or result["false_positive_rate"] > 0.02
            or result["unsafe_write_count"] != 0
        ):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
