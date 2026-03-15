"""Example: user-defined declarative transforms + CSV export."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from data_ingestion.pipeline import stream_transformed_records

QUERY = "data engineering"
MAX_PAGES = 5
OUTPUT_PATH = "data/all_sources_ingestion.csv"
START_DATE = None
END_DATE = None
INCLUDE_NEWSAPI = False

TRANSFORM_SPEC: dict[str, Any] = {
    "transforms": [
        {
            "op": "require_fields",
            "fields": ["title", "url"],
        },
        {
            "op": "include_terms",
            "terms": ["data", "engineering", "pipeline", "etl", "ai"],
            "fields": ["title", "abstract", "topic"],
        },
        {
            "op": "exclude_terms",
            "terms": ["sports", "celebrity"],
            "fields": ["title", "abstract"],
        },
        {
            "op": "assign_topic_from_terms",
            "terms": ["data engineering", "etl", "ai"],
            "fields": ["title", "abstract", "topic"],
        },
        {
            "op": "dedupe",
            "keys": ["source", "external_id", "url"],
        },
    ]
}


def with_dates(config: dict[str, Any]) -> dict[str, Any]:
    if START_DATE is not None:
        config["start_date"] = START_DATE
    if END_DATE is not None:
        config["end_date"] = END_DATE
    return config


def main() -> None:
    fetcher_specs: list[dict[str, Any]] = [
        {
            "source": "openalex",
            "config": with_dates(
                {
                    "query": QUERY,
                    "max_pages": MAX_PAGES,
                    "per_page": 100,
                }
            ),
        },
        {
            "source": "crossref",
            "config": with_dates({"query": QUERY, "max_pages": MAX_PAGES, "rows": 100}),
        },
        {
            "source": "hackernews",
            "config": with_dates(
                {"query": QUERY, "max_pages": MAX_PAGES, "hits_per_page": 100}
            ),
        },
        {
            "source": "federalregister",
            "config": with_dates(
                {"query": QUERY, "max_pages": MAX_PAGES, "per_page": 100}
            ),
        },
    ]

    if INCLUDE_NEWSAPI:
        fetcher_specs.append(
            {
                "source": "newsapi",
                "config": with_dates(
                    {
                        "query": QUERY,
                        "max_pages": MAX_PAGES,
                        "page_size": 100,
                        "language": "en",
                    }
                ),
            }
        )

    output = Path(OUTPUT_PATH)
    output.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "source",
        "external_id",
        "title",
        "published_date",
        "url",
        "topic",
        "raw_json",
    ]

    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for source, record in stream_transformed_records(
            fetcher_specs,
            transform_spec=TRANSFORM_SPEC,
            start_date=START_DATE,
            end_date=END_DATE,
        ):
            writer.writerow(
                {
                    "source": source,
                    "external_id": record.external_id,
                    "title": record.title,
                    "published_date": (
                        record.published_date.isoformat()
                        if record.published_date is not None
                        else None
                    ),
                    "url": record.url,
                    "topic": record.topic,
                    "raw_json": json.dumps(record.raw_payload, ensure_ascii=False),
                }
            )

    print(f"Wrote CSV to {output}")


if __name__ == "__main__":
    main()
