#!/usr/bin/env python3
"""Enforce one `{% docs <fetcher.py> %}` block per concrete fetcher."""

from __future__ import annotations

import re
import sys
from pathlib import Path


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


def _collect_doc_blocks(content: str) -> set[str]:
    pattern = re.compile(r"{%\s*docs\s+([^%\s]+)\s*%}")
    return {match.group(1).strip() for match in pattern.finditer(content)}


def main() -> int:
    fetchers = _collect_fetcher_files()
    if not DOCS_FILE.is_file():
        print(f"error: missing docs file: {DOCS_FILE}", file=sys.stderr)
        return 1

    content = DOCS_FILE.read_text(encoding="utf-8")
    documented = _collect_doc_blocks(content)

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

    print(f"Fetcher docs check passed: {len(fetchers)} fetcher(s) documented.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
