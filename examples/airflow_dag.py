"""Example Airflow DAG with user-defined declarative transform rules."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion.pipeline import stream_transformed_records

FETCHER_SPECS = [
    {
        "source": "openalex",
        "config": {
            "query": "data engineering",
            "max_pages": 1,
            "per_page": 100,
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
        },
    },
    {
        "source": "crossref",
        "config": {
            "query": "data engineering",
            "max_pages": 1,
            "rows": 100,
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
        },
    },
]

TRANSFORM_SPEC: dict[str, Any] = {
    "transforms": [
        {"op": "require_fields", "fields": ["title", "url"]},
        {
            "op": "include_terms",
            "terms": ["data", "engineering", "pipeline", "etl"],
            "fields": ["title", "abstract", "topic"],
        },
        {"op": "dedupe", "keys": ["source", "external_id", "url"]},
    ]
}


def ingest_sources(
    fetcher_specs: list[dict[str, Any]], output_path: str
) -> dict[str, Any]:
    """Write transformed records to JSONL using library transform engine."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    total_records = 0
    by_source: dict[str, int] = {}

    with out.open("w", encoding="utf-8") as fh:
        for source, record in stream_transformed_records(
            fetcher_specs,
            transform_spec=TRANSFORM_SPEC,
        ):
            fh.write(
                json.dumps(
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
                        "raw_payload": record.raw_payload,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            total_records += 1
            by_source[source] = by_source.get(source, 0) + 1

    return {
        "total_records": total_records,
        "by_source": by_source,
        "raw_mode": False,
    }


with DAG(
    dag_id="data_ingestion_declarative_transform",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="ingest_sources",
        python_callable=ingest_sources,
        op_kwargs={
            "fetcher_specs": FETCHER_SPECS,
            "output_path": "/tmp/lightweight_ingestion_transformed.jsonl",
        },
    )
