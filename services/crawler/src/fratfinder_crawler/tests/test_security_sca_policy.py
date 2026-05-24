from __future__ import annotations

import json
from pathlib import Path
from datetime import date
from uuid import uuid4

import pytest

from fratfinder_crawler.security.sca_policy import (
    SecurityPolicyError,
    combine_scan_files,
    evaluate_vulnerability_policy,
    load_vulnerability_ignores,
    render_evidence_report,
)


def _workspace_tmp() -> Path:
    path = Path("tmp") / "security-sca-tests" / uuid4().hex
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_ignore(path, *, expires: str = "2099-01-01", reason: str = "Temporary accepted risk with documented remediation."):
    path.write_text(
        "\n".join(
            [
                "ignores:",
                '  - package: "demo-package"',
                '    vulnerability_id: "CVE-2099-0001"',
                f'    reason: "{reason}"',
                '    owner: "security-team"',
                f'    expires: "{expires}"',
            ]
        ),
        encoding="utf-8",
    )


def _write_scan(path, *, severity: str = "Critical"):
    path.write_text(
        json.dumps(
            {
                "matches": [
                    {
                        "vulnerability": {"id": "CVE-2099-0001", "severity": severity},
                        "artifact": {"name": "demo-package"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )


def test_vulnerability_ignore_schema_requires_all_fields():
    tmp_path = _workspace_tmp()
    ignore_file = tmp_path / "vulnerability-ignores.yml"
    ignore_file.write_text("ignores:\n  - package: demo\n", encoding="utf-8")

    with pytest.raises(SecurityPolicyError, match="missing required fields"):
        load_vulnerability_ignores(ignore_file)


def test_expired_vulnerability_ignore_is_rejected():
    tmp_path = _workspace_tmp()
    ignore_file = tmp_path / "vulnerability-ignores.yml"
    _write_ignore(ignore_file, expires="2020-01-01")

    with pytest.raises(SecurityPolicyError, match="expired"):
        load_vulnerability_ignores(ignore_file, today=date(2026, 5, 23))


def test_unignored_critical_vulnerability_fails_policy():
    tmp_path = _workspace_tmp()
    ignore_file = tmp_path / "vulnerability-ignores.yml"
    ignore_file.write_text("ignores:\n", encoding="utf-8")
    scan_file = tmp_path / "scan.json"
    _write_scan(scan_file)

    with pytest.raises(SecurityPolicyError, match="Unignored critical"):
        evaluate_vulnerability_policy(scan_paths=[scan_file], ignore_file=ignore_file)


def test_valid_critical_ignore_passes_policy():
    tmp_path = _workspace_tmp()
    ignore_file = tmp_path / "vulnerability-ignores.yml"
    _write_ignore(ignore_file)
    scan_file = tmp_path / "scan.json"
    _write_scan(scan_file)

    result = evaluate_vulnerability_policy(scan_paths=[scan_file], ignore_file=ignore_file)

    assert result["pass"] is True
    assert result["critical_policy_result"] == "pass"
    assert result["ignored_vulnerability_count"] == 1


def test_high_vulnerability_warns_by_default_but_can_fail_policy():
    tmp_path = _workspace_tmp()
    ignore_file = tmp_path / "vulnerability-ignores.yml"
    ignore_file.write_text("ignores:\n", encoding="utf-8")
    scan_file = tmp_path / "scan.json"
    _write_scan(scan_file, severity="High")

    result = evaluate_vulnerability_policy(scan_paths=[scan_file], ignore_file=ignore_file)
    assert result["pass"] is True
    assert result["high_policy_result"] == "warn"

    with pytest.raises(SecurityPolicyError, match="Unignored high"):
        evaluate_vulnerability_policy(scan_paths=[scan_file], ignore_file=ignore_file, fail_on_high=True)


def test_evidence_report_renders_component_and_vulnerability_totals():
    tmp_path = _workspace_tmp()
    ignore_file = tmp_path / "vulnerability-ignores.yml"
    _write_ignore(ignore_file)
    scan_file = tmp_path / "scan.json"
    _write_scan(scan_file, severity="High")
    sbom_file = tmp_path / "app.cdx.json"
    sbom_file.write_text(
        json.dumps({"bomFormat": "CycloneDX", "components": [{"name": "next"}, {"name": "react"}]}),
        encoding="utf-8",
    )
    output = tmp_path / "sbom-evidence.md"

    rendered = render_evidence_report(
        sbom_paths=[sbom_file],
        scan_paths=[scan_file],
        ignore_file=ignore_file,
        output_path=output,
        commit_sha="abc123",
        workflow_run_id="42",
        syft_version="syft 1.0.0",
        scanner_version="grype 1.0.0",
    )

    assert output.exists()
    assert "SBOM And SCA Evidence" in rendered
    assert "`2` components" in rendered
    assert "High: `1`" in rendered
    assert "syft 1.0.0" in rendered


def test_combine_scan_files_expands_globs():
    tmp_path = _workspace_tmp()
    scan_file = tmp_path / "grype-app.json"
    _write_scan(scan_file, severity="High")
    output = tmp_path / "combined.json"

    combined = combine_scan_files([str(tmp_path / "grype-*.json")], output)

    assert output.exists()
    assert len(combined["scans"]) == 1
    combined_payload = json.loads(output.read_text(encoding="utf-8"))
    assert len(combined_payload["scans"]) == 1


def test_evidence_report_renders_even_when_critical_policy_would_fail():
    tmp_path = _workspace_tmp()
    ignore_file = tmp_path / "vulnerability-ignores.yml"
    ignore_file.write_text("ignores:\n", encoding="utf-8")
    scan_file = tmp_path / "scan.json"
    _write_scan(scan_file, severity="Critical")
    sbom_file = tmp_path / "app.cdx.json"
    sbom_file.write_text(json.dumps({"bomFormat": "CycloneDX", "components": [{"name": "next"}]}), encoding="utf-8")
    output = tmp_path / "sbom-evidence.md"

    rendered = render_evidence_report(
        sbom_paths=[sbom_file],
        scan_paths=[scan_file],
        ignore_file=ignore_file,
        output_path=output,
    )

    assert output.exists()
    assert "Critical vulnerability policy result: `fail`" in rendered
    assert "Final result: `fail`" in rendered
    assert "Unignored Critical Vulnerabilities" in rendered
