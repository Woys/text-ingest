# Massive Data Ingestion

A Python ingestion library that collects data from multiple APIs/websites and writes normalized records to JSONL, CSV, or Parquet.

All fetchers ingest into a single model: `NormalizedRecord` ([src/data_ingestion/models.py](src/data_ingestion/models.py)).

## Install

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from data_ingestion.config import JsonlSinkConfig, PipelineConfig
from data_ingestion.factories import build_fetchers
from data_ingestion.pipeline import DataDumperPipeline
from data_ingestion.sinks.jsonl import JsonlSink

fetchers = build_fetchers(
    [
        {"source": "openalex", "config": {"query": "data engineering", "max_pages": 1}},
        {"source": "crossref", "config": {"query": "data engineering", "max_pages": 1}},
        {"source": "website", "config": {"site_url": "https://aws.amazon.com/blogs/aws/", "max_items": 20}},
    ]
)

sink = JsonlSink(JsonlSinkConfig(output_file="data/output.jsonl", append=False))
pipeline = DataDumperPipeline(sink=sink, config=PipelineConfig(fail_fast=True))
summary = pipeline.run(fetchers)
print(summary.model_dump())
```

## Supported Fetchers

- `openalex`
- `crossref`
- `newsapi`
- `hackernews`
- `federalregister`
- `website` (RSS/Atom + autodiscovery)
- `website_html` (non-RSS HTML fallback)
- `wikipedia`
- `reddit`
- `github`
- `stackexchange`
- `openlibrary`
- `googlenews`
- `guardian`

See full config contracts in [docs/FETCHER_DOCS.md](docs/FETCHER_DOCS.md).

## Unified Output Schema

Every fetcher emits the same fields defined in `NormalizedRecord` ([src/data_ingestion/models.py](src/data_ingestion/models.py)):

- `source`
- `external_id`
- `title`
- `authors`
- `published_date`
- `url`
- `abstract`
- `full_text`
- `full_text_url`
- `topic`
- `record_type`
- `fetched_at`
- `raw_payload`

## Sinks

- JSONL
- CSV
- Parquet
- Full-text JSONL

Sink contracts: [docs/SINK_DOCS.md](docs/SINK_DOCS.md)

## Examples

Examples are in `examples/`, including:

- `csv_export.py`
- `parquet_export.py`
- `full_text_export.py`
- `industry_trend_search.py`
- `website_blog_export.py`
- `airflow_dag.py`
- `dlt_source_ingestion.py`

## Quality Commands

Run the full project pipeline:

```bash
make all
```

Or step-by-step:

```bash
make format
make lint
make docs-check
make typecheck
make test
```

## Documentation

- Index: [docs/DOCUMENTATION.md](docs/DOCUMENTATION.md)
- DLT source pack: `assets/DLT_SOURCES.md`
- Architecture/API/operations/release docs: `assets/`

## Adding a New Fetcher

1. Add a config model in `src/data_ingestion/config.py` and extend `FetcherSpec.source`.
2. Implement a fetcher in `src/data_ingestion/fetchers/<name>.py` with `@register_fetcher("<name>")`.
3. Import it in `src/data_ingestion/fetchers/__init__.py` and `src/data_ingestion/factories.py`.
4. Add tests.
5. Add/maintain its entry in `docs/FETCHER_DOCS.md` so `make docs-check` passes.
