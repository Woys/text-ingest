from __future__ import annotations

import data_ingestion.query_utils as query_utils
from data_ingestion.models import NormalizedRecord
from data_ingestion.query_utils import (
    _partial_ratio,
    _token_set_ratio,
    build_search_text,
    fuzzy_match_record,
)


def test_build_search_text_lowercases_and_joins_fields() -> None:
    record = NormalizedRecord(
        source="openalex",
        title="AI Systems",
        abstract="Reliable Pipelines",
        full_text="Detailed Notes",
        raw_payload={},
    )

    text = build_search_text(record)
    assert text == "ai systems reliable pipelines detailed notes"


def test_fuzzy_match_record_returns_false_for_empty_text() -> None:
    record = NormalizedRecord(source="openalex", title=None, raw_payload={})
    assert fuzzy_match_record(record, query="ai") is False


def test_fuzzy_match_record_true_when_no_candidates_and_text_exists() -> None:
    record = NormalizedRecord(source="openalex", title="some text", raw_payload={})
    assert fuzzy_match_record(record, query=None, fuzzy_terms=None) is True


def test_fuzzy_match_record_true_on_substring_hit() -> None:
    record = NormalizedRecord(
        source="openalex",
        title="Data engineering handbook",
        raw_payload={},
    )
    assert fuzzy_match_record(record, query="engineering") is True


def test_fuzzy_match_record_true_on_partial_ratio_branch(monkeypatch) -> None:
    record = NormalizedRecord(
        source="openalex",
        title="machine learning in practice",
        raw_payload={},
    )

    monkeypatch.setattr(query_utils, "_partial_ratio", lambda *_: 90.0)
    monkeypatch.setattr(query_utils, "_token_set_ratio", lambda *_: 0.0)

    assert fuzzy_match_record(record, query="ml", threshold=80) is True


def test_fuzzy_match_record_false_when_threshold_not_reached() -> None:
    record = NormalizedRecord(source="openalex", title="abc", raw_payload={})
    assert (
        fuzzy_match_record(record, query="completely different", threshold=95) is False
    )


def test_partial_ratio_and_token_set_ratio_return_float() -> None:
    assert isinstance(_partial_ratio("abc", "abc xyz"), float)
    assert isinstance(_token_set_ratio("a b c", "c b a"), float)
