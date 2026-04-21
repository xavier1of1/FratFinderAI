from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from fratfinder_crawler.status.campus_discovery import CampusSourceDocument, build_campus_status_index
from fratfinder_crawler.status.decision_engine import decide_chapter_status
from fratfinder_crawler.status.national_capabilities import infer_national_status_from_page


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _fixture_text(relative_path: str) -> str:
    fixture_path = _repo_root() / "services" / "crawler" / "src" / "fratfinder_crawler" / "tests" / "fixtures" / relative_path
    return fixture_path.read_text(encoding="utf-8")


def evaluate_case(case: dict[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    fixture_paths = list(case.get("fixture_paths") or [])
    status_documents: list[CampusSourceDocument] = []
    national_evidence = None

    for fixture_path in fixture_paths:
        text = _fixture_text(str(fixture_path))
        if str(fixture_path).startswith("status_pages/"):
            status_documents.append(
                CampusSourceDocument(
                    page_url=str(case.get("school_url") or f"https://example.edu/{Path(str(fixture_path)).stem}"),
                    title=str(case.get("title") or Path(str(fixture_path)).stem.replace("_", " ")),
                    text=text,
                    html=text,
                )
            )
        elif str(fixture_path).startswith("national_directories/"):
            national_evidence = infer_national_status_from_page(
                fraternity_name=str(case["fraternity_name"]),
                school_name=str(case["school_name"]),
                page_url=str(case.get("national_url") or f"https://national.example.org/{Path(str(fixture_path)).stem}"),
                title=str(Path(str(fixture_path)).stem.replace("_", " ")),
                text=text,
                html=text,
            )

    start = time.perf_counter()
    index = build_campus_status_index(school_name=str(case["school_name"]), documents=status_documents)
    decision = decide_chapter_status(
        fraternity_name=str(case["fraternity_name"]),
        fraternity_slug=str(case.get("fraternity_slug") or str(case["fraternity_name"]).lower().replace(" ", "-")),
        school_name=str(case["school_name"]),
        index=index,
        national_evidence=national_evidence,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    expected_final_status = str(case["expected_final_status"])
    expected_school_status = str(case["expected_school_recognition_status"])
    passed = decision.final_status == expected_final_status and decision.school_recognition_status == expected_school_status
    result = {
        "caseId": case["case_id"],
        "expectedFinalStatus": expected_final_status,
        "expectedSchoolRecognitionStatus": expected_school_status,
        "actualFinalStatus": str(decision.final_status),
        "actualSchoolRecognitionStatus": str(decision.school_recognition_status),
        "passed": passed,
        "sourcesFetched": len(status_documents),
        "decisionLatencyMs": round(elapsed_ms, 4),
        "evidenceCount": len(decision.evidence_ids),
        "conflictCount": len(decision.conflict_flags),
    }
    return result, decision.model_dump(mode="json")


def evaluate_goldset(*, goldset_path: Path) -> dict[str, object]:
    cases = [
        json.loads(line)
        for line in goldset_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    results = []
    decisions = []
    for case in cases:
        result, decision = evaluate_case(case)
        results.append(result)
        decisions.append(decision)

    total_cases = len(results)
    decided_cases = sum(1 for item in results if item["actualFinalStatus"] in {"active", "inactive"})
    review_cases = sum(1 for item in results if item["actualFinalStatus"] == "review")
    unknown_cases = sum(1 for item in results if item["actualFinalStatus"] == "unknown")
    correct_final_status = sum(1 for item in results if item["expectedFinalStatus"] == item["actualFinalStatus"])
    incorrect_final_status = total_cases - correct_final_status
    correct_decided_cases = sum(
        1
        for item in results
        if item["actualFinalStatus"] in {"active", "inactive"} and item["expectedFinalStatus"] == item["actualFinalStatus"]
    )
    false_active_count = sum(1 for item in results if item["expectedFinalStatus"] == "inactive" and item["actualFinalStatus"] == "active")
    false_inactive_count = sum(1 for item in results if item["expectedFinalStatus"] == "active" and item["actualFinalStatus"] == "inactive")
    accuracy_on_decided_cases = round(correct_decided_cases / decided_cases, 4) if decided_cases else 0.0
    overall_accuracy_with_review_as_non_error = round(correct_final_status / total_cases, 4) if total_cases else 0.0
    review_rate = round(review_cases / total_cases, 4) if total_cases else 0.0
    unknown_rate = round(unknown_cases / total_cases, 4) if total_cases else 0.0
    avg_sources_fetched = round(sum(item["sourcesFetched"] for item in results) / total_cases, 4) if total_cases else 0.0
    avg_search_queries = 0.0
    avg_decision_latency_ms = round(sum(item["decisionLatencyMs"] for item in results) / total_cases, 4) if total_cases else 0.0
    evidence_coverage_rate = round(sum(1 for decision in decisions if decision["final_status"] in {"active", "inactive"} and decision["evidence_ids"]) / max(1, decided_cases), 4)
    conflict_detection_rate = round(sum(1 for decision in decisions if decision["conflict_flags"]) / total_cases, 4) if total_cases else 0.0

    return {
        "total_cases": total_cases,
        "decided_cases": decided_cases,
        "review_cases": review_cases,
        "unknown_cases": unknown_cases,
        "correct_final_status": correct_final_status,
        "incorrect_final_status": incorrect_final_status,
        "false_active_count": false_active_count,
        "false_inactive_count": false_inactive_count,
        "accuracy_on_decided_cases": accuracy_on_decided_cases,
        "overall_accuracy_with_review_as_non_error": overall_accuracy_with_review_as_non_error,
        "review_rate": review_rate,
        "unknown_rate": unknown_rate,
        "avg_sources_fetched": avg_sources_fetched,
        "avg_search_queries": avg_search_queries,
        "avg_decision_latency_ms": avg_decision_latency_ms,
        "evidence_coverage_rate": evidence_coverage_rate,
        "conflict_detection_rate": conflict_detection_rate,
        "results": results,
    }


def _assert_thresholds(metrics: dict[str, object]) -> None:
    if float(metrics["accuracy_on_decided_cases"]) < 0.97:
        raise SystemExit("decided_accuracy threshold failed")
    if float(metrics["evidence_coverage_rate"]) < 0.95:
        raise SystemExit("evidence_coverage_rate threshold failed")
    if float(metrics["false_active_count"]) > 0:
        raise SystemExit("false_active_count threshold failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--goldset", required=True)
    parser.add_argument("--fail-on-threshold", action="store_true")
    args = parser.parse_args()

    metrics = evaluate_goldset(goldset_path=Path(args.goldset))
    print(json.dumps(metrics, indent=2))
    if args.fail_on_threshold:
        _assert_thresholds(metrics)


if __name__ == "__main__":
    main()
