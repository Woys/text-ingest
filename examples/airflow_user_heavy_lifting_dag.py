"""Airflow DAG with user-defined transform rules executed by library engine.

This DAG demonstrates a split of responsibilities:
- User defines transformation logic declaratively in TRANSFORM_SPEC.
- Library executes filtering, topic assignment, and dedupe.
- Airflow tasks stay lightweight and orchestration-focused.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_ingestion.pipeline import stream_transformed_records

FETCHER_SPECS: list[dict[str, Any]] = [
    {
        "source": "openalex",
        "config": {
            "query": "artificial intelligence",
            "max_pages": 2,
            "per_page": 100,
            "start_date": "2026-01-01",
            "end_date": "2026-01-07",
        },
    },
    {
        "source": "crossref",
        "config": {
            "query": "artificial intelligence",
            "max_pages": 2,
            "rows": 100,
            "start_date": "2026-01-01",
            "end_date": "2026-01-07",
        },
    },
]

TRANSFORM_SPEC: dict[str, Any] = {
    "transforms": [
        {
            "op": "require_fields",
            "fields": ["title", "url"],
        },
        {
            "op": "include_terms",
            "terms": ["ai", "artificial intelligence", "machine learning"],
            "fields": ["title", "abstract", "topic"],
        },
        {
            "op": "exclude_terms",
            "terms": ["sports", "celebrity"],
            "fields": ["title", "abstract"],
        },
        {
            "op": "assign_topic_from_terms",
            "terms": ["artificial intelligence", "machine learning", "ai"],
            "fields": ["title", "abstract", "topic"],
        },
        {
            "op": "dedupe",
            "keys": ["source", "external_id", "url"],
        },
    ]
}

BASE_DIR = Path("/tmp/mdi_airflow_user_pipeline")


def extract_curated(fetcher_specs: list[dict[str, Any]], run_id: str) -> str:
    """Task 1: extraction + transform engine + curated JSONL output."""
    run_dir = BASE_DIR / run_id
    curated_dir = run_dir / "curated"
    curated_dir.mkdir(parents=True, exist_ok=True)

    curated_path = curated_dir / "records.jsonl"
    records_by_source: Counter[str] = Counter()
    total_records = 0

    with curated_path.open("w", encoding="utf-8") as out:
        for source, record in stream_transformed_records(
            fetcher_specs,
            transform_spec=TRANSFORM_SPEC,
        ):
            out.write(
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
                        "topic": record.topic,
                        "raw_payload": record.raw_payload,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            records_by_source[source] += 1
            total_records += 1

    manifest = {
        "run_id": run_id,
        "curated_path": str(curated_path),
        "records_by_source": dict(records_by_source),
        "total_records": total_records,
    }
    manifest_path = run_dir / "curation_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return str(manifest_path)


def aggregate_metrics(run_id: str) -> str:
    """Task 2: user-defined metrics export (CSV)."""
    run_dir = BASE_DIR / run_id
    curated_path = run_dir / "curated" / "records.jsonl"
    metrics_path = run_dir / "metrics.csv"

    by_source: Counter[str] = Counter()
    with curated_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            by_source[row["source"]] += 1

    with metrics_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["source", "record_count"])
        writer.writeheader()
        for source, count in sorted(by_source.items()):
            writer.writerow({"source": source, "record_count": count})

    return str(metrics_path)


def build_pipeline_run_id(ts_nodash: str) -> str:
    """Use Airflow execution timestamp as deterministic run partition."""
    return ts_nodash


with DAG(
    dag_id="mdi_user_transform_engine_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mdi", "declarative-transform", "airflow"],
) as dag:
    build_run_id = PythonOperator(
        task_id="build_run_id",
        python_callable=build_pipeline_run_id,
        op_kwargs={"ts_nodash": "{{ ts_nodash }}"},
    )

    curated = PythonOperator(
        task_id="extract_curated",
        python_callable=extract_curated,
        op_kwargs={
            "fetcher_specs": FETCHER_SPECS,
            "run_id": "{{ ti.xcom_pull(task_ids='build_run_id') }}",
        },
    )

    aggregate = PythonOperator(
        task_id="aggregate_metrics",
        python_callable=aggregate_metrics,
        op_kwargs={"run_id": "{{ ti.xcom_pull(task_ids='build_run_id') }}"},
    )

    build_run_id >> curated >> aggregate
