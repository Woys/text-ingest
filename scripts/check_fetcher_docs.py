#!/usr/bin/env python3
"""Enforce and validate fetcher docs entries using Pydantic schema."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from pydantic import BaseModel, Field, ValidationError, field_validator

ROOT = Path(__file__).resolve().parent.parent
FETCHERS_DIR = ROOT / "src" / "data_ingestion" / "fetchers"
DOCS_FILE = ROOT / "docs" / "FETCHER_DOCS.md"
EXCLUDED_FETCHER_FILES = {"__init__.py", "base.py"}


def _collect_fetcher_files() -> list[str]:
    return sorted(
        path.name
        for path in FETCHERS_DIR.glob("*.py")
        if path.name not in EXCLUDED_FETCHER_FILES
    )


JSON_BLOCK_PATTERN = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL)


class DocsEntry(BaseModel):
    file_name_py: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    required_config: list[str] = Field(..., min_length=1)
    optional_config: list[str] = Field(..., min_length=1)
    notes: str | None = None

    @field_validator("file_name_py")
    @classmethod
    def _validate_file_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned.endswith(".py"):
            raise ValueError("file_name_py must end with .py")
        return cleaned

    @field_validator("name")
    @classmethod
    def _clean_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("name must not be blank")
        return cleaned

    @field_validator("required_config", "optional_config", mode="before")
    @classmethod
    def _coerce_list(cls, value: object) -> list[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [str(item) for item in value]
        raise ValueError("must be a list of strings")

    @field_validator("required_config", "optional_config")
    @classmethod
    def _normalize_list(cls, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in values:
            normalized = item.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            cleaned.append(normalized)
        if not cleaned:
            raise ValueError("list must include at least one non-empty value")
        return cleaned

    @field_validator("notes")
    @classmethod
    def _clean_notes(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


def _collect_doc_blocks(content: str) -> dict[str, DocsEntry]:
    match = JSON_BLOCK_PATTERN.search(content)
    if not match:
        raise ValueError("docs file must include a ```json ... ``` block")

    json_blob = match.group(1).strip()
    try:
        raw_entries = json.loads(json_blob)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON docs payload: {exc}") from exc

    if not isinstance(raw_entries, list):
        raise ValueError("docs JSON payload must be a list")

    blocks: dict[str, DocsEntry] = {}
    for raw_entry in raw_entries:
        try:
            entry = DocsEntry.model_validate(raw_entry)
        except ValidationError as exc:
            raise ValueError(f"invalid docs entry: {exc}") from exc
        if entry.file_name_py in blocks:
            raise ValueError(f"duplicate docs entry for {entry.file_name_py}")
        blocks[entry.file_name_py] = entry
    return blocks


def main() -> int:
    fetchers = _collect_fetcher_files()
    if not DOCS_FILE.is_file():
        print(f"error: missing docs file: {DOCS_FILE}", file=sys.stderr)
        return 1

    content = DOCS_FILE.read_text(encoding="utf-8")
    try:
        documented = _collect_doc_blocks(content)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    missing = [name for name in fetchers if name not in documented]
    orphaned = sorted(name for name in documented if name not in fetchers)

    if missing:
        print("error: missing fetcher docs blocks for:", file=sys.stderr)
        for name in missing:
            print(f"  - {name}", file=sys.stderr)
        return 1

    if orphaned:
        print("error: docs blocks found for non-existent fetchers:", file=sys.stderr)
        for name in orphaned:
            print(f"  - {name}", file=sys.stderr)
        return 1

    print(
        "Fetcher docs check passed: "
        f"{len(fetchers)} fetcher(s) documented with valid schema."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
