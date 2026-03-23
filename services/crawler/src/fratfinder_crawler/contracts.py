import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


class ContractValidator:
    def __init__(self) -> None:
        repo_root = Path(__file__).resolve().parents[4]
        schemas_dir = repo_root / "packages" / "contracts" / "schemas"

        self._chapter_validator = Draft202012Validator(
            json.loads((schemas_dir / "canonical-chapter.schema.json").read_text(encoding="utf-8"))
        )
        self._provenance_validator = Draft202012Validator(
            json.loads((schemas_dir / "chapter-provenance.schema.json").read_text(encoding="utf-8"))
        )
        self._review_validator = Draft202012Validator(
            json.loads((schemas_dir / "review-item-payload.schema.json").read_text(encoding="utf-8"))
        )
        self._field_job_validator = Draft202012Validator(
            json.loads((schemas_dir / "field-job-payload.schema.json").read_text(encoding="utf-8"))
        )

    def validate_chapter(self, payload: dict[str, Any]) -> None:
        self._chapter_validator.validate(payload)

    def validate_provenance(self, payload: dict[str, Any]) -> None:
        self._provenance_validator.validate(payload)

    def validate_review_item(self, payload: dict[str, Any]) -> None:
        self._review_validator.validate(payload)

    def validate_field_job(self, payload: dict[str, Any]) -> None:
        self._field_job_validator.validate(payload)