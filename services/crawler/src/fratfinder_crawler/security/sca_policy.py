from __future__ import annotations

import argparse
import glob
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


_VULN_ID_RE = re.compile(r"^(CVE-|GHSA-|PYSEC-|OSV-|RUSTSEC-|ALAS|DSA-|ELSA-|USN-|GO-)", re.IGNORECASE)
_SEVERITIES = ("Critical", "High", "Medium", "Low", "Negligible", "Unknown")


class SecurityPolicyError(ValueError):
    pass


@dataclass(frozen=True)
class VulnerabilityIgnore:
    package: str
    vulnerability_id: str
    reason: str
    owner: str
    expires: date

    @property
    def key(self) -> tuple[str, str]:
        return (self.package.lower(), self.vulnerability_id.upper())


def _strip_yaml_value(value: str) -> str:
    value = value.strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value


def load_vulnerability_ignores(path: str | Path, *, today: date | None = None) -> list[VulnerabilityIgnore]:
    """Load the small project-owned YAML schema without adding a runtime YAML dependency."""

    ignore_path = Path(path)
    if not ignore_path.exists():
        raise SecurityPolicyError(f"Ignore file not found: {ignore_path}")

    lines = ignore_path.read_text(encoding="utf-8").splitlines()
    if not any(line.strip() == "ignores:" for line in lines):
        raise SecurityPolicyError("Ignore file must contain a top-level `ignores:` key.")

    raw_items: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or line == "ignores:":
            continue
        if line.startswith("- "):
            if current is not None:
                raw_items.append(current)
            current = {}
            line = line[2:].strip()
            if not line:
                continue
        if ":" not in line:
            raise SecurityPolicyError(f"Malformed ignore line: {raw_line}")
        if current is None:
            raise SecurityPolicyError(f"Ignore entries must be list items: {raw_line}")
        key, value = line.split(":", 1)
        current[key.strip()] = _strip_yaml_value(value)
    if current is not None:
        raw_items.append(current)

    today_value = today or datetime.now(timezone.utc).date()
    ignores: list[VulnerabilityIgnore] = []
    for index, item in enumerate(raw_items, start=1):
        missing = [key for key in ("package", "vulnerability_id", "reason", "owner", "expires") if not item.get(key, "").strip()]
        if missing:
            raise SecurityPolicyError(f"Ignore entry {index} is missing required fields: {', '.join(missing)}")

        vuln_id = item["vulnerability_id"].strip()
        if not _VULN_ID_RE.match(vuln_id):
            raise SecurityPolicyError(f"Ignore entry {index} has unsupported vulnerability_id `{vuln_id}`.")

        reason = item["reason"].strip()
        if len(reason) < 20:
            raise SecurityPolicyError(f"Ignore entry {index} reason must be at least 20 meaningful characters.")

        try:
            expires = date.fromisoformat(item["expires"].strip())
        except ValueError as exc:
            raise SecurityPolicyError(f"Ignore entry {index} expires must be an ISO date.") from exc
        if expires < today_value:
            raise SecurityPolicyError(f"Ignore entry {index} expired on {expires.isoformat()}.")

        ignores.append(
            VulnerabilityIgnore(
                package=item["package"].strip(),
                vulnerability_id=vuln_id,
                reason=reason,
                owner=item["owner"].strip(),
                expires=expires,
            )
        )
    return ignores


def _iter_grype_matches(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for match in payload.get("matches") or []:
        if isinstance(match, dict):
            vuln = match.get("vulnerability") or {}
            artifact = match.get("artifact") or {}
            yield {
                "id": str(vuln.get("id") or ""),
                "severity": str(vuln.get("severity") or "Unknown").title(),
                "package": str(artifact.get("name") or ""),
                "source": "grype",
            }


def _iter_osv_vulnerabilities(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for result in payload.get("results") or []:
        package = ((result.get("package") or {}).get("name") or result.get("package") or "")
        for vuln in result.get("vulnerabilities") or result.get("vulns") or []:
            if isinstance(vuln, dict):
                severity = "Unknown"
                severities = vuln.get("severity") or []
                if isinstance(severities, list) and severities:
                    severity = str((severities[0] or {}).get("score") or (severities[0] or {}).get("type") or "Unknown")
                yield {
                    "id": str(vuln.get("id") or ""),
                    "severity": severity.title(),
                    "package": str(package),
                    "source": "osv",
                }


def load_vulnerability_records(scan_paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in _expand_scan_paths(scan_paths):
        scan_path = Path(path)
        if not scan_path.exists():
            continue
        payload = _load_json(scan_path)
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    records.extend(_iter_grype_matches(item))
                    records.extend(_iter_osv_vulnerabilities(item))
        elif isinstance(payload, dict):
            records.extend(_iter_grype_matches(payload))
            records.extend(_iter_osv_vulnerabilities(payload))
            for item in payload.get("scans") or []:
                if isinstance(item, dict):
                    records.extend(_iter_grype_matches(item))
                    records.extend(_iter_osv_vulnerabilities(item))
    return [record for record in records if record.get("id") and record.get("package")]


def _expand_scan_paths(scan_paths: Iterable[str | Path]) -> list[Path]:
    expanded: list[Path] = []
    for path in scan_paths:
        raw = str(path)
        matches = [Path(match) for match in glob.glob(raw)]
        expanded.extend(matches or [Path(raw)])
    return expanded


def combine_scan_files(scan_paths: Iterable[str | Path], output_path: str | Path) -> dict[str, Any]:
    scans = []
    for path in _expand_scan_paths(scan_paths):
        scan_path = Path(path)
        if scan_path.exists():
            scans.append({"path": str(scan_path), "payload": _load_json(scan_path)})
    combined = {"schema": "fratfinder-security-vulnerability-scan-v1", "generated_at": datetime.now(timezone.utc).isoformat(), "scans": [item["payload"] for item in scans]}
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(json.dumps(combined, indent=2, sort_keys=True), encoding="utf-8")
    return combined


def evaluate_vulnerability_policy(
    *,
    scan_paths: Iterable[str | Path],
    ignore_file: str | Path,
    today: date | None = None,
    fail_on_high: bool = False,
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    ignores = load_vulnerability_ignores(ignore_file, today=today)
    ignore_keys = {ignore.key for ignore in ignores}
    records = load_vulnerability_records(scan_paths)
    totals = {severity: 0 for severity in _SEVERITIES}
    unignored_critical: list[dict[str, Any]] = []
    high_warnings: list[dict[str, Any]] = []

    for record in records:
        severity = str(record.get("severity") or "Unknown").title()
        if severity not in totals:
            severity = "Unknown"
        totals[severity] += 1
        key = (str(record.get("package") or "").lower(), str(record.get("id") or "").upper())
        ignored = key in ignore_keys
        if severity == "Critical" and not ignored:
            unignored_critical.append(record)
        if severity == "High" and not ignored:
            high_warnings.append(record)

    high_policy_failure = bool(fail_on_high and high_warnings)
    result = {
        "pass": not unignored_critical and not high_policy_failure,
        "vulnerability_totals": totals,
        "ignored_vulnerability_count": len(ignores),
        "critical_policy_result": "pass" if not unignored_critical else "fail",
        "high_policy_result": "fail" if high_policy_failure else "warn",
        "unignored_critical": unignored_critical,
        "high_warnings": high_warnings,
    }
    if unignored_critical and raise_on_failure:
        raise SecurityPolicyError(f"Unignored critical vulnerabilities found: {len(unignored_critical)}")
    if high_policy_failure and raise_on_failure:
        raise SecurityPolicyError(f"Unignored high vulnerabilities found: {len(high_warnings)}")
    return result


def _component_count(sbom_path: Path) -> int:
    payload = _load_json(sbom_path)
    if payload.get("bomFormat") != "CycloneDX":
        raise SecurityPolicyError(f"{sbom_path} is not a CycloneDX SBOM.")
    return len(payload.get("components") or [])


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def render_evidence_report(
    *,
    sbom_paths: Iterable[str | Path],
    scan_paths: Iterable[str | Path],
    ignore_file: str | Path,
    output_path: str | Path,
    commit_sha: str | None = None,
    workflow_run_id: str | None = None,
    syft_version: str | None = None,
    scanner_version: str | None = None,
) -> str:
    policy = evaluate_vulnerability_policy(scan_paths=scan_paths, ignore_file=ignore_file, raise_on_failure=False)
    sbom_counts = {str(path): _component_count(Path(path)) for path in sbom_paths if Path(path).exists()}
    generated_at = datetime.now(timezone.utc).isoformat()
    lines = [
        "# SBOM And SCA Evidence",
        "",
        f"- Generated at: `{generated_at}`",
        f"- Commit SHA: `{commit_sha or 'local'}`",
        f"- Workflow run ID: `{workflow_run_id or 'local'}`",
        f"- Critical vulnerability policy result: `{policy['critical_policy_result']}`",
        f"- Ignored vulnerability count: `{policy['ignored_vulnerability_count']}`",
        f"- Expired ignore count: `0`",
        f"- Final result: `{'pass' if policy['pass'] else 'fail'}`",
        "",
        "## Tool Versions",
        "",
        f"- Syft: `{syft_version or 'unknown-local'}`",
        f"- Grype/OSV Scanner: `{scanner_version or 'unknown-local'}`",
        "- Policy renderer: `fratfinder-security-policy-v1`",
        "",
        "## SBOM Files",
        "",
    ]
    for path, count in sbom_counts.items():
        lines.append(f"- `{path}`: `{count}` components")
    lines.extend(["", "## Vulnerabilities By Severity", ""])
    for severity in _SEVERITIES:
        lines.append(f"- {severity}: `{policy['vulnerability_totals'].get(severity, 0)}`")
    if policy["high_warnings"]:
        lines.extend(["", "## High Severity Warnings", ""])
        for warning in policy["high_warnings"][:25]:
            lines.append(f"- `{warning['id']}` in `{warning['package']}`")
    if policy["unignored_critical"]:
        lines.extend(["", "## Unignored Critical Vulnerabilities", ""])
        for critical in policy["unignored_critical"][:25]:
            lines.append(f"- `{critical['id']}` in `{critical['package']}`")
    rendered = "\n".join(lines) + "\n"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(rendered, encoding="utf-8")
    return rendered


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FratFinderAI SBOM/SCA policy helper")
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate-ignores")
    validate.add_argument("--ignore-file", default=".security/vulnerability-ignores.yml")
    combine = sub.add_parser("combine-scans")
    combine.add_argument("--output", required=True)
    combine.add_argument("scan_files", nargs="*")
    evaluate = sub.add_parser("evaluate")
    evaluate.add_argument("--ignore-file", default=".security/vulnerability-ignores.yml")
    evaluate.add_argument("--scan", action="append", required=True)
    evaluate.add_argument("--fail-on-high", action="store_true")
    render = sub.add_parser("render-evidence")
    render.add_argument("--ignore-file", default=".security/vulnerability-ignores.yml")
    render.add_argument("--scan", action="append", required=True)
    render.add_argument("--sbom", action="append", required=True)
    render.add_argument("--output", required=True)
    render.add_argument("--commit-sha")
    render.add_argument("--workflow-run-id")
    render.add_argument("--syft-version")
    render.add_argument("--scanner-version")
    args = parser.parse_args(argv)
    try:
        if args.command == "validate-ignores":
            load_vulnerability_ignores(args.ignore_file)
        elif args.command == "combine-scans":
            combine_scan_files(args.scan_files, args.output)
        elif args.command == "evaluate":
            evaluate_vulnerability_policy(scan_paths=args.scan, ignore_file=args.ignore_file, fail_on_high=args.fail_on_high)
        elif args.command == "render-evidence":
            render_evidence_report(
                sbom_paths=args.sbom,
                scan_paths=args.scan,
                ignore_file=args.ignore_file,
                output_path=args.output,
                commit_sha=args.commit_sha,
                workflow_run_id=args.workflow_run_id,
                syft_version=args.syft_version,
                scanner_version=args.scanner_version,
            )
    except SecurityPolicyError as exc:
        print(f"security policy failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
