"""Cross-source schema consistency tests."""

from __future__ import annotations

import json
from datetime import date

import pytest

from data_ingestion.config import CrossRefConfig, NewsApiConfig, OpenAlexConfig
from data_ingestion.fetchers.crossref import CrossRefFetcher
from data_ingestion.fetchers.newsapi import NewsApiFetcher
from data_ingestion.fetchers.openalex import OpenAlexFetcher
from data_ingestion.models import NormalizedRecord, RecordType

EXPECTED_KEYS = frozenset(
    {
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
)


def _openalex_record():
    fetcher = OpenAlexFetcher(OpenAlexConfig(query="test", max_pages=1))
    return fetcher.normalize(
        {
            "id": "https://openalex.org/W999",
            "title": "Schema Test Paper",
            "publication_date": "2025-01-10",
            "authorships": [{"author": {"display_name": "Dr. A"}}],
            "doi": "https://doi.org/10.x/test",
            "abstract_inverted_index": {"Test": [0]},
        }
    )


def _crossref_record():
    fetcher = CrossRefFetcher(CrossRefConfig(query="test", max_pages=1))
    return fetcher.normalize(
        {
            "DOI": "10.x/crossref",
            "URL": "https://doi.org/10.x/crossref",
            "title": ["Schema Test Paper"],
            "author": [{"given": "Dr.", "family": "B"}],
            "published": {"date-parts": [[2025, 1, 10]]},
            "type": "journal-article",
        }
    )


def _newsapi_record(monkeypatch):
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    fetcher = NewsApiFetcher(NewsApiConfig(query="test", max_pages=1))
    return fetcher.normalize(
        {
            "title": "Schema Test News",
            "author": "Dr. C",
            "description": "A test news item.",
            "url": "https://news.example.com/test",
            "publishedAt": "2025-01-10T08:00:00Z",
        }
    )


@pytest.mark.parametrize("get_record", [_openalex_record, _crossref_record])
def test_normalized_record_has_all_expected_keys(get_record) -> None:
    rec = get_record()
    assert isinstance(rec, NormalizedRecord)
    line = json.loads(rec.to_json_line())
    assert set(line.keys()) == EXPECTED_KEYS


def test_newsapi_record_has_all_expected_keys(monkeypatch) -> None:
    rec = _newsapi_record(monkeypatch)
    line = json.loads(rec.to_json_line())
    assert set(line.keys()) == EXPECTED_KEYS


@pytest.mark.parametrize("get_record", [_openalex_record, _crossref_record])
def test_published_date_is_iso_string_or_null(get_record) -> None:
    rec = get_record()
    line = json.loads(rec.to_json_line())
    pd = line["published_date"]
    if pd is not None:
        date.fromisoformat(pd)


def test_newsapi_published_date_is_iso_string_or_null(monkeypatch) -> None:
    rec = _newsapi_record(monkeypatch)
    line = json.loads(rec.to_json_line())
    pd = line["published_date"]
    if pd is not None:
        date.fromisoformat(pd)


@pytest.mark.parametrize("get_record", [_openalex_record, _crossref_record])
def test_authors_is_list_of_strings(get_record) -> None:
    rec = get_record()
    assert isinstance(rec.authors, list)
    assert all(isinstance(a, str) for a in rec.authors)


def test_newsapi_authors_is_list_of_strings(monkeypatch) -> None:
    rec = _newsapi_record(monkeypatch)
    assert isinstance(rec.authors, list)
    assert all(isinstance(a, str) for a in rec.authors)


@pytest.mark.parametrize("get_record", [_openalex_record, _crossref_record])
def test_record_type_is_valid_enum_value(get_record) -> None:
    rec = get_record()
    assert rec.record_type in RecordType.__members__.values()


def test_newsapi_record_type_is_news(monkeypatch) -> None:
    rec = _newsapi_record(monkeypatch)
    assert rec.record_type == RecordType.NEWS


@pytest.mark.parametrize("get_record", [_openalex_record, _crossref_record])
def test_raw_payload_is_non_empty_dict(get_record) -> None:
    rec = get_record()
    assert isinstance(rec.raw_payload, dict)
    assert len(rec.raw_payload) > 0


def test_newsapi_raw_payload_is_non_empty_dict(monkeypatch) -> None:
    rec = _newsapi_record(monkeypatch)
    assert isinstance(rec.raw_payload, dict)
    assert len(rec.raw_payload) > 0
