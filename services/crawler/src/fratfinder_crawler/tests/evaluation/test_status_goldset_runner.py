from __future__ import annotations

from pathlib import Path

from fratfinder_crawler.status.evaluate_goldset import evaluate_goldset


def test_status_goldset_runner_emits_metrics():
    goldset = Path(__file__).resolve().parents[6] / "services" / "crawler" / "evaluation" / "chapter_status_goldset.jsonl"
    metrics = evaluate_goldset(goldset_path=goldset)
    assert metrics["total_cases"] >= 5
    assert "accuracy_on_decided_cases" in metrics
    assert "evidence_coverage_rate" in metrics
