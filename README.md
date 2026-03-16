# Text Ingest

A Python ingestion library that collects text data from multiple APIs and provides a stable, typed ingestion surface for downstream systems.

It supports three styles:

- Lightweight streaming (raw payloads)
- Declarative transform engine (user-defined rules, library-executed transforms)
- Convenience pipeline (normalized records + built-in sinks)

## Install

```bash
pip install -e ".[dev]"
```

## Documentation

Comprehensive documentation lives in `docs/`:

- Documentation index: `docs/DOCUMENTATION.md`
- Architecture: `assets/ARCHITECTURE.md`
- API contract and compatibility: `assets/API_CONTRACT.md`
- Operations and deployment: `assets/OPERATIONS.md`
- Release process: `assets/RELEASE_PROCESS.md`
- Open source standards: `assets/OPEN_SOURCE_STANDARDS.md`
- Governance: `assets/GOVERNANCE.md`
- Testing and quality strategy: `assets/TESTING_AND_QUALITY.md`

Repository policies:

- Contributing: `CONTRIBUTING.md`
- Security: `SECURITY.md`
- Code of conduct: `CODE_OF_CONDUCT.md`
- Changelog: `CHANGELOG.md`

## Quick Start

### Lightweight Usage

```python
from data_ingestion.pipeline import stream_records

for source, raw_item in stream_records(
    [
        {"source": "openalex", "config": {"query": "data engineering", "max_pages": 1}},
        {"source": "crossref", "config": {"query": "data engineering", "max_pages": 1}},
    ],
    raw=True,
):
    handle_raw_payload(source, raw_item)
```

### Declarative Transform Engine (Recommended)

Users define transform behavior as a spec instead of writing transform functions.

```python
from data_ingestion.pipeline import stream_transformed_records

transform_spec = {
    "version": 1,
    "transforms": [
        {"op": "require_fields", "fields": ["title", "url"]},
        {
            "op": "include_terms",
            "terms": ["data", "engineering"],
            "fields": ["title", "abstract", "topic"],
        },
        {"op": "dedupe", "keys": ["source", "external_id", "url"]},
    ],
}

for source, record in stream_transformed_records(
    [
        {"source": "openalex", "config": {"query": "data engineering", "max_pages": 1}},
        {"source": "crossref", "config": {"query": "data engineering", "max_pages": 1}},
    ],
    transform_spec=transform_spec,
):
    print(source, record.title)
```

Compatibility notes:

- `version` defaults to `1` if omitted.
- legacy `steps` alias is accepted for `transforms`.

### Managed Pipeline Usage

```python
from data_ingestion.pipeline import run_to_jsonl

summary = run_to_jsonl(
    fetcher_specs=[
        {"source": "openalex", "config": {"query": "data engineering", "max_pages": 1}},
        {"source": "crossref", "config": {"query": "data engineering", "max_pages": 1}},
    ],
    output_file="data/output.jsonl",
    transform_spec={"transforms": [{"op": "dedupe", "keys": ["source", "external_id", "url"]}]},
    checkpoint_path="data/checkpoint.json",
    resume=True,
)
print(summary)
```

## Deployment

1. Run quality checks before release:

```bash
make all
```

2. Build distributable artifacts:

```bash
python -m build
ls -lh dist/
```

3. Install wheel in runtime environment:

```bash
pip install dist/massive_data_ingestion-*.whl
```

4. Run CLI ingestion:

```bash
mdi-run run \
  --spec-file deploy/spec.json \
  --output-file /var/data/mdi/output.jsonl \
  --overwrite \
  --log-level INFO
```

5. Optional full-text output:

```bash
mdi-run run \
  --spec-file deploy/spec.json \
  --output-file /var/data/mdi/output.jsonl \
  --full-text-output-file /var/data/mdi/full_text.jsonl
```

6. Runtime notes:

- Set `NEWSAPI_KEY` if `newsapi` is used.
- Ensure output/checkpoint directories are writable.

For production practices (monitoring, checkpoints, failure recovery), see `assets/OPERATIONS.md`.

## Notebooks

- `examples/lightweight_processing_visualization.ipynb`
- `examples/regular_processing_visualization.ipynb`

## Adding a New Source

1. Define config model in `src/data_ingestion/config.py` and add source name in `FetcherSpec`.
2. Implement fetcher in `src/data_ingestion/fetchers/`:
- `fetch_pages()` for raw page streaming (required)
- `normalize()` for normalized path
3. Register fetcher imports in:
- `src/data_ingestion/fetchers/__init__.py`
- `src/data_ingestion/factories.py`

Example skeleton:

```python
from typing import Any, Iterator

from data_ingestion.config import MyNewApiConfig
from data_ingestion.fetchers.base import BaseFetcher
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher


@register_fetcher("mynewapi")
class MyNewApiFetcher(BaseFetcher):
    config_model = MyNewApiConfig

    @property
    def source_name(self) -> str:
        return "mynewapi"

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        yield []

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        return NormalizedRecord(
            source=self.source_name,
            title=item.get("headline"),
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )
```

## Industry Trend and Text Search

Analyze exported datasets for topic trends and text matches.

```bash
mdi-analyze \
  --input-file data/all_sources_ingestion.csv \
  --topic "data engineering" \
  --text-query "agent" \
  --lookback-days 90 \
  --window-days 7 \
  --limit 50
```

Python API:

```python
from data_ingestion.analysis import analyze_topic_trends, search_industry_export

matches = search_industry_export(
    "data/all_sources_ingestion.csv",
    topic_query="data engineering",
    text_query="agent",
    limit=100,
)

trend = analyze_topic_trends(
    "data/all_sources_ingestion.csv",
    topic_query="data engineering",
    text_query="agent",
    lookback_days=90,
    window_days=7,
)
```

Example script: `examples/industry_trend_search.py`
