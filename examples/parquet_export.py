"""Example: user-defined declarative transforms + Parquet export."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from data_ingestion.pipeline import stream_transformed_records

TRANSFORM_SPEC: dict[str, Any] = {
    "transforms": [
        {
            "op": "require_fields",
            "fields": ["title", "url"],
        },
        {
            "op": "include_terms",
            "terms": ["data", "engineering", "ai", "etl"],
            "fields": ["title", "abstract", "topic"],
        },
        {
            "op": "dedupe",
            "keys": ["source", "external_id", "url"],
        },
    ]
}

QUERY = "data engineering"
MAX_PAGES = 1
LANGUAGES: list[str] = []  # e.g. ["en"]


def with_filters(config: dict[str, Any]) -> dict[str, Any]:
    if LANGUAGES:
        config["languages"] = LANGUAGES
    return config


def main() -> None:
    fetcher_specs = [
        {
            "source": "openalex",
            "config": with_filters(
                {"query": QUERY, "max_pages": MAX_PAGES, "per_page": 5}
            ),
        },
        {
            "source": "crossref",
            "config": with_filters({"query": QUERY, "max_pages": MAX_PAGES, "rows": 5}),
        },
    ]

    rows: list[dict[str, Any]] = []
    for source, record in stream_transformed_records(
        fetcher_specs,
        transform_spec=TRANSFORM_SPEC,
    ):
        rows.append(
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
                "raw": record.raw_payload,
            }
        )

    table = pa.Table.from_pylist(rows)
    output_path = Path("data/raw_ingestion.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy")

    print(f"Wrote {table.num_rows} rows to {output_path}")


if __name__ == "__main__":
    main()
