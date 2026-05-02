import json
from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from data_ingestion.config import (
    JsonlSinkConfig,
    PipelineConfig,
    RuntimeOptimizationConfig,
)
from data_ingestion.exceptions import PipelineError
from data_ingestion.models import NormalizedRecord
from data_ingestion.pipeline import DataDumperPipeline
from data_ingestion.sinks.jsonl import JsonlSink


class _FakeFetcher:
    def __init__(
        self,
        source_name: str,
        records: list[NormalizedRecord],
        config: object | None = None,
    ) -> None:
        self._name = source_name
        self._records = records
        self.config = config if config is not None else SimpleNamespace()

    @property
    def source_name(self) -> str:
        return self._name

    def fetch_all(self) -> Iterator[NormalizedRecord]:
        yield from self._records


class _FailingFetcher:
    @property
    def source_name(self) -> str:
        return "broken"

    def fetch_all(self) -> Iterator[NormalizedRecord]:
        raise RuntimeError("boom")
        yield  # makes it a generator


def _make_sink(tmp_path):
    return JsonlSink(
        JsonlSinkConfig(output_file=str(tmp_path / "out.jsonl"), append=False)
    )


def test_pipeline_aggregates_from_multiple_sources(tmp_path) -> None:
    pipeline = DataDumperPipeline(sink=_make_sink(tmp_path))

    f1 = _FakeFetcher(
        "openalex",
        [
            NormalizedRecord(
                source="openalex", external_id="1", title="A", raw_payload={}
            ),
            NormalizedRecord(
                source="openalex", external_id="2", title="B", raw_payload={}
            ),
        ],
    )
    f2 = _FakeFetcher(
        "crossref",
        [
            NormalizedRecord(
                source="crossref", external_id="doi-1", title="C", raw_payload={}
            )
        ],
    )

    summary = pipeline.run([f1, f2])

    assert summary.total_records == 3
    assert summary.by_source == {"openalex": 2, "crossref": 1}
    assert summary.by_source_stats["openalex"].seen == 2
    assert summary.by_source_stats["openalex"].kept == 2
    assert summary.by_source_stats["crossref"].seen == 1
    assert summary.output_target == str(tmp_path / "out.jsonl")

    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    records = [json.loads(line) for line in lines]
    assert len(records) == 3
    expected_keys = {
        "source",
        "external_id",
        "title",
        "authors",
        "published_date",
        "url",
        "abstract",
        "full_text",
        "full_text_url",
        "topic",
        "record_type",
        "fetched_at",
        "raw_payload",
    }
    for rec in records:
        assert expected_keys == set(rec.keys())
        assert rec["topic"] is None


def test_pipeline_preserves_fetcher_topic(tmp_path) -> None:
    pipeline = DataDumperPipeline(sink=_make_sink(tmp_path))

    fetcher = _FakeFetcher(
        "newsapi",
        [
            NormalizedRecord(
                source="newsapi",
                external_id="1",
                title="Some story",
                topic="BBC News",
                raw_payload={},
            )
        ],
        config=SimpleNamespace(query=None, topic_include=[], topic_exclude=[]),
    )

    summary = pipeline.run([fetcher])
    assert summary.total_records == 1

    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    records = [json.loads(line) for line in lines]
    assert records[0]["topic"] == "BBC News"


def test_pipeline_applies_topic_filters(tmp_path) -> None:
    pipeline = DataDumperPipeline(sink=_make_sink(tmp_path))

    topic_config = SimpleNamespace(
        query="oncology",
        topic_include=["cancer"],
        topic_exclude=["mouse"],
    )
    fetcher = _FakeFetcher(
        "openalex",
        [
            NormalizedRecord(
                source="openalex",
                external_id="1",
                title="Cancer treatment update",
                abstract="Promising phase-2 results",
                raw_payload={},
            ),
            NormalizedRecord(
                source="openalex",
                external_id="2",
                title="Cancer model",
                abstract="Validated in mouse trials",
                raw_payload={},
            ),
            NormalizedRecord(
                source="openalex",
                external_id="3",
                title="Astronomy update",
                abstract="New telescope imagery",
                raw_payload={},
            ),
        ],
        config=topic_config,
    )

    summary = pipeline.run([fetcher])
    assert summary.total_records == 1
    assert summary.by_source == {"openalex": 1}
    assert summary.by_source_stats["openalex"].seen == 3
    assert summary.by_source_stats["openalex"].dropped_by_topic == 2

    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    records = [json.loads(line) for line in lines]
    assert len(records) == 1
    assert records[0]["external_id"] == "1"
    assert records[0]["topic"] == "cancer"


def test_fail_fast_raises_pipeline_error(tmp_path) -> None:
    pipeline = DataDumperPipeline(
        sink=_make_sink(tmp_path), config=PipelineConfig(fail_fast=True)
    )
    with pytest.raises(PipelineError):
        pipeline.run([_FailingFetcher()])


def test_continue_on_error_records_failure(tmp_path) -> None:
    pipeline = DataDumperPipeline(
        sink=_make_sink(tmp_path), config=PipelineConfig(fail_fast=False)
    )
    summary = pipeline.run([_FailingFetcher()])

    assert summary.total_records == 0
    assert "broken" in summary.failed_sources
    assert "boom" in summary.failed_sources["broken"]


def test_sink_closed_even_when_fail_fast(tmp_path) -> None:
    sink = _make_sink(tmp_path)
    pipeline = DataDumperPipeline(sink=sink, config=PipelineConfig(fail_fast=True))

    with pytest.raises(PipelineError):
        pipeline.run([_FailingFetcher()])

    assert sink._handle is None or sink._handle.closed


def test_pipeline_applies_transform_engine(tmp_path) -> None:
    from data_ingestion.transforms import TransformationEngine

    sink = _make_sink(tmp_path)
    engine = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "include_terms",
                    "terms": ["ai"],
                    "fields": ["title"],
                },
                {
                    "op": "set_field",
                    "field": "topic",
                    "value": "library-transform",
                },
            ]
        }
    )
    pipeline = DataDumperPipeline(sink=sink, transform_engine=engine)

    fetcher = _FakeFetcher(
        "openalex",
        [
            NormalizedRecord(
                source="openalex",
                external_id="1",
                title="AI progress",
                raw_payload={},
            ),
            NormalizedRecord(
                source="openalex",
                external_id="2",
                title="Classical mechanics",
                raw_payload={},
            ),
        ],
    )

    summary = pipeline.run([fetcher])
    assert summary.total_records == 1
    assert summary.by_source_stats["openalex"].dropped_by_transform == 1

    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    records = [json.loads(line) for line in lines]
    assert len(records) == 1
    assert records[0]["external_id"] == "1"
    assert records[0]["topic"] == "library-transform"


def test_pipeline_drops_raw_payload_by_default(tmp_path) -> None:
    pipeline = DataDumperPipeline(sink=_make_sink(tmp_path))
    fetcher = _FakeFetcher(
        "openalex",
        [
            NormalizedRecord(
                source="openalex",
                external_id="1",
                title="A",
                raw_payload={"large": "payload"},
            )
        ],
    )

    pipeline.run([fetcher])

    line = (tmp_path / "out.jsonl").read_text().splitlines()[0]
    assert json.loads(line)["raw_payload"] == {}


def test_pipeline_preserves_raw_payload_when_configured(tmp_path) -> None:
    config = PipelineConfig(runtime=RuntimeOptimizationConfig(write_raw_payload=True))
    pipeline = DataDumperPipeline(sink=_make_sink(tmp_path), config=config)
    fetcher = _FakeFetcher(
        "openalex",
        [
            NormalizedRecord(
                source="openalex",
                external_id="1",
                title="A",
                raw_payload={"large": "payload"},
            )
        ],
    )

    pipeline.run([fetcher])

    line = (tmp_path / "out.jsonl").read_text().splitlines()[0]
    assert json.loads(line)["raw_payload"] == {"large": "payload"}


def test_pipeline_preserves_raw_payload_when_transform_uses_it(tmp_path) -> None:
    from data_ingestion.transforms import TransformationEngine

    engine = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "include_terms",
                    "terms": ["needle"],
                    "fields": ["raw_payload.text"],
                }
            ]
        }
    )
    pipeline = DataDumperPipeline(sink=_make_sink(tmp_path), transform_engine=engine)
    fetcher = _FakeFetcher(
        "openalex",
        [
            NormalizedRecord(
                source="openalex",
                external_id="1",
                title="A",
                raw_payload={"text": "needle"},
            )
        ],
    )

    pipeline.run([fetcher])

    line = (tmp_path / "out.jsonl").read_text().splitlines()[0]
    assert json.loads(line)["raw_payload"] == {"text": "needle"}


def test_resume_requires_checkpoint_path(tmp_path) -> None:
    sink = _make_sink(tmp_path)
    with pytest.raises(ValueError, match="requires checkpoint_path"):
        DataDumperPipeline(sink=sink, resume=True)


def test_pipeline_resume_from_checkpoint_skips_completed_sources(tmp_path) -> None:
    checkpoint_path = tmp_path / "pipeline.checkpoint.json"

    f1 = _FakeFetcher(
        "openalex",
        [
            NormalizedRecord(
                source="openalex", external_id="1", title="A", raw_payload={}
            )
        ],
    )
    f2 = _FakeFetcher(
        "crossref",
        [
            NormalizedRecord(
                source="crossref", external_id="2", title="B", raw_payload={}
            )
        ],
    )

    first_run = DataDumperPipeline(
        sink=_make_sink(tmp_path),
        checkpoint_path=str(checkpoint_path),
        resume=False,
    )
    first_summary = first_run.run([f1, f2])
    assert first_summary.total_records == 2
    assert checkpoint_path.exists()

    second_run = DataDumperPipeline(
        sink=_make_sink(tmp_path),
        checkpoint_path=str(checkpoint_path),
        resume=True,
    )
    second_summary = second_run.run([f1, f2])

    assert second_summary.total_records == 0
    assert second_summary.resumed_from_checkpoint is True
    assert second_summary.checkpoint_path == str(checkpoint_path)
    assert second_summary.checkpoint_entries == 2
    assert second_summary.by_source_stats["openalex"].checkpoint_skipped == 1
    assert second_summary.by_source_stats["crossref"].checkpoint_skipped == 1
