"""Example: produce a full-text candidate queue via declarative transforms."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from data_ingestion.pipeline import stream_transformed_records

OUTPUT_PATH = Path("data/full_text_candidates.jsonl")

TRANSFORM_SPEC: dict[str, Any] = {
    "transforms": [
        {"op": "require_fields", "fields": ["title", "url"]},
        {
            "op": "include_terms",
            "terms": ["data", "engineering", "pipeline", "etl", "ai"],
            "fields": ["title", "abstract", "topic"],
        },
        {"op": "dedupe", "keys": ["source", "external_id", "url"]},
    ]
}


def main() -> None:
    fetcher_specs = [
        {
            "source": "openalex",
            "config": {"query": "data engineering", "max_pages": 10},
        },
        {
            "source": "crossref",
            "config": {"query": "data engineering", "max_pages": 10},
        },
        {
            "source": "hackernews",
            "config": {"query": "data engineering", "max_pages": 10},
        },
    ]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        for source, record in stream_transformed_records(
            fetcher_specs,
            transform_spec=TRANSFORM_SPEC,
            start_date="2026-01-01",
            end_date="2026-01-31",
        ):
            row = {
                "source": source,
                "external_id": record.external_id,
                "title": record.title,
                "candidate_full_text_url": record.full_text_url or record.url,
                "raw_payload": record.raw_payload,
            }
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Wrote full-text candidate queue to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
