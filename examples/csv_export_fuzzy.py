"""Example: fuzzy configs + declarative transform engine + CSV export."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from typing import Any

from data_ingestion.pipeline import stream_transformed_records

TODAY = date.today().isoformat()
OUTPUT_PATH = "data/cancer_prevention_updates_today.csv"

TRANSFORM_SPEC: dict[str, Any] = {
    "transforms": [
        {"op": "require_fields", "fields": ["title", "url"]},
        {
            "op": "include_terms",
            "terms": ["cancer", "prevention", "screening", "oncology"],
            "fields": ["title", "abstract", "topic"],
        },
        {"op": "dedupe", "keys": ["source", "external_id", "url"]},
    ]
}


def main() -> None:
    fetcher_specs: list[dict[str, Any]] = [
        {
            "source": "openalex",
            "config": {
                "query": None,
                "start_date": TODAY,
                "end_date": TODAY,
                "search_mode": "fuzzy_local",
                "fuzzy_terms": [
                    "cancer prevention",
                    "cancer risk reduction",
                    "prevent cancer",
                    "early cancer screening",
                    "cancer prophylaxis",
                ],
                "fuzzy_threshold": 85,
                "max_pages": 10,
                "per_page": 200,
            },
        },
        {
            "source": "crossref",
            "config": {
                "query": None,
                "start_date": TODAY,
                "end_date": TODAY,
                "search_mode": "fuzzy_local",
                "date_mode": "publication",
                "fuzzy_terms": [
                    "cancer prevention",
                    "cancer risk reduction",
                    "prevent cancer",
                    "early cancer screening",
                ],
                "rows": 1000,
                "max_pages": 10,
            },
        },
        {
            "source": "hackernews",
            "config": {
                "query": None,
                "start_date": TODAY,
                "end_date": TODAY,
                "search_mode": "fuzzy_local",
                "hn_item_type": "story",
                "fuzzy_terms": [
                    "cancer prevention",
                    "cancer vaccine",
                    "cancer screening",
                ],
                "hits_per_page": 500,
                "max_pages": 5,
            },
        },
        {
            "source": "federalregister",
            "config": {
                "query": None,
                "start_date": TODAY,
                "end_date": TODAY,
                "search_mode": "fuzzy_local",
                "fuzzy_terms": [
                    "cancer prevention",
                    "cancer screening",
                    "oncology prevention",
                ],
                "max_pages": 5,
                "per_page": 100,
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

    print(f"Wrote fuzzy CSV to {output}")


if __name__ == "__main__":
    main()
