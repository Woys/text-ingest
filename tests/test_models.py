"""Tests for NormalizedRecord and PipelineSummary."""

import json
from datetime import date

from data_ingestion.models import (
    NormalizedRecord,
    PipelineSummary,
    RecordType,
    SourceRunStats,
)


def test_normalized_record_fields(sample_record) -> None:
    assert sample_record.source == "openalex"
    assert sample_record.external_id == "https://openalex.org/W1"
    assert sample_record.title == "A study on cancer prevention"
    assert sample_record.authors == ["Alice Smith", "Bob Jones"]
    assert sample_record.published_date == date(2024, 6, 15)
    assert sample_record.url == "https://doi.org/10.1000/test"
    assert sample_record.abstract == "A short abstract."
    assert sample_record.topic is None
    assert sample_record.record_type == RecordType.ARTICLE
    assert isinstance(sample_record.raw_payload, dict)


def test_to_json_line_is_single_line(sample_record) -> None:
    line = sample_record.to_json_line()
    assert line
    assert "\n" not in line


def test_to_json_line_round_trips(sample_record) -> None:
    line = sample_record.to_json_line()
    parsed = json.loads(line)
    assert parsed["source"] == "openalex"
    assert parsed["title"] == "A study on cancer prevention"
    assert parsed["authors"] == ["Alice Smith", "Bob Jones"]
    assert parsed["published_date"] == "2024-06-15"
    assert parsed["topic"] is None
    assert parsed["record_type"] == "article"


def test_normalized_record_default_authors() -> None:
    rec = NormalizedRecord(source="test", raw_payload={})
    assert rec.authors == []


def test_normalized_record_default_record_type() -> None:
    rec = NormalizedRecord(source="test", raw_payload={})
    assert rec.record_type == RecordType.ARTICLE


def test_record_type_enum_values() -> None:
    assert RecordType.ARTICLE.value == "article"
    assert RecordType.NEWS.value == "news"
    assert RecordType.PREPRINT.value == "preprint"


def test_pipeline_summary_defaults() -> None:
    summary = PipelineSummary()
    assert summary.total_records == 0
    assert summary.by_source == {}
    assert summary.failed_sources == {}
    assert summary.by_source_stats == {}
    assert summary.output_target is None
    assert summary.resumed_from_checkpoint is False
    assert summary.checkpoint_path is None
    assert summary.checkpoint_entries == 0


def test_source_run_stats_defaults() -> None:
    stats = SourceRunStats()
    assert stats.seen == 0
    assert stats.kept == 0
    assert stats.dropped_by_topic == 0
    assert stats.dropped_by_transform == 0
    assert stats.checkpoint_skipped == 0
