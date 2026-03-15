"""Example: yesterday pull with declarative transform engine + CSV export."""

from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from data_ingestion.pipeline import stream_transformed_records

YERSTODAY = (date.today() - timedelta(days=1)).isoformat()
OUTPUT_PATH = "data/all_sources_yerstoday.csv"

TRANSFORM_SPEC: dict[str, Any] = {
    "transforms": [
        {"op": "require_fields", "fields": ["title", "url"]},
        {"op": "dedupe", "keys": ["source", "external_id", "url"]},
    ]
}


def main() -> None:
    fetcher_specs = [
        {
            "source": "openalex",
            "config": {
                "query": None,
                "start_date": YERSTODAY,
                "end_date": YERSTODAY,
                "search_mode": "date_only",
                "max_pages": 10,
                "per_page": 200,
            },
        },
        {
            "source": "crossref",
            "config": {
                "query": None,
                "start_date": YERSTODAY,
                "end_date": YERSTODAY,
                "search_mode": "date_only",
                "date_mode": "publication",
                "rows": 1000,
                "max_pages": 10,
            },
        },
        {
            "source": "hackernews",
            "config": {
                "query": None,
                "start_date": YERSTODAY,
                "end_date": YERSTODAY,
                "search_mode": "date_only",
                "hn_item_type": "story",
                "hits_per_page": 500,
                "max_pages": 10,
            },
        },
        {
            "source": "federalregister",
            "config": {
                "query": None,
                "start_date": YERSTODAY,
                "end_date": YERSTODAY,
                "search_mode": "date_only",
                "per_page": 100,
                "max_pages": 10,
            },
        },
    ]

    output = Path(OUTPUT_PATH)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["source", "title", "published_date", "url", "raw_json"],
        )
        writer.writeheader()

        for source, record in stream_transformed_records(
            fetcher_specs,
            transform_spec=TRANSFORM_SPEC,
        ):
            writer.writerow(
                {
                    "source": source,
                    "title": record.title,
                    "published_date": (
                        record.published_date.isoformat()
                        if record.published_date is not None
                        else None
                    ),
                    "url": record.url,
                    "raw_json": json.dumps(record.raw_payload, ensure_ascii=False),
                }
            )

    print(f"Wrote yesterday CSV to {output}")


if __name__ == "__main__":
    main()
