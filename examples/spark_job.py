"""Example Spark job using declarative transforms and DataFrame creation."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from data_ingestion.adapters.spark import records_to_dataframe
from data_ingestion.pipeline import stream_transformed_records

spark = SparkSession.builder.appName("mdi-spark-declarative").getOrCreate()

FETCHER_SPECS = [
    {
        "source": "openalex",
        "config": {
            "query": "data engineering",
            "max_pages": 2,
            "per_page": 100,
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
        },
    },
    {
        "source": "crossref",
        "config": {
            "query": "data engineering",
            "max_pages": 2,
            "rows": 100,
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
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

rows = [
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
    }
    for source, record in stream_transformed_records(
        FETCHER_SPECS,
        transform_spec=TRANSFORM_SPEC,
    )
]

# User still controls Spark schema/table behavior; transform logic stays declarative.
df = records_to_dataframe(spark, rows)

print("Rows after declarative transforms:", len(rows))
df.printSchema()
df.show(20, truncate=False)
