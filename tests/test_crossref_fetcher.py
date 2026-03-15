"""Tests for CrossRefFetcher — pagination and normalization."""

from datetime import date

import pytest

from data_ingestion.config import CrossRefConfig
from data_ingestion.fetchers.crossref import CrossRefFetcher
from data_ingestion.models import RecordType


def _make_item(**kwargs):
    base = {
        "DOI": "10.1000/abc",
        "URL": "https://doi.org/10.1000/abc",
        "title": ["Test Article"],
        "author": [
            {"given": "Alice", "family": "Smith"},
            {"given": "Bob", "family": "Jones"},
        ],
        "published": {"date-parts": [[2024, 6, 15]]},
        "type": "journal-article",
        "subject": ["Oncology"],
        "abstract": "<jats:p>A short <b>abstract</b>.</jats:p>",
    }
    base.update(kwargs)
    return base


@pytest.fixture()
def fetcher():
    return CrossRefFetcher(CrossRefConfig(query="test", max_pages=1))


def test_normalize_all_fields(fetcher) -> None:
    item = _make_item()
    rec = fetcher.normalize(item)

    assert rec.source == "crossref"
    assert rec.external_id == "10.1000/abc"
    assert rec.title == "Test Article"
    assert rec.authors == ["Alice Smith", "Bob Jones"]
    assert rec.published_date == date(2024, 6, 15)
    assert rec.url == "https://doi.org/10.1000/abc"
    assert rec.abstract == "A short abstract."
    assert rec.topic == "Oncology"
    assert rec.record_type == RecordType.ARTICLE
    assert rec.raw_payload == item


def test_normalize_strips_jats_xml(fetcher) -> None:
    item = _make_item(
        abstract="<jats:sec><jats:title>Background</jats:title>Text.</jats:sec>"
    )
    rec = fetcher.normalize(item)
    assert rec.abstract == "BackgroundText."


def test_normalize_preprint_type(fetcher) -> None:
    item = _make_item(type="posted-content")
    rec = fetcher.normalize(item)
    assert rec.record_type == RecordType.PREPRINT


def test_normalize_unknown_type_defaults_to_article(fetcher) -> None:
    item = _make_item(type="something-new")
    rec = fetcher.normalize(item)
    assert rec.record_type == RecordType.ARTICLE


def test_normalize_partial_date(fetcher) -> None:
    item = _make_item()
    item["published"] = {"date-parts": [[2024]]}
    rec = fetcher.normalize(item)
    assert rec.published_date == date(2024, 1, 1)


def test_normalize_missing_date(fetcher) -> None:
    item = _make_item()
    item["published"] = {"date-parts": [[]]}
    rec = fetcher.normalize(item)
    assert rec.published_date is None


def test_streams_records(fake_response_factory, monkeypatch) -> None:
    payload = {"message": {"items": [_make_item(DOI="a"), _make_item(DOI="b")]}}
    config = CrossRefConfig(query="cancer prevention", max_pages=1, rows=2)
    fetcher = CrossRefFetcher(config)
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda url, params, timeout: fake_response_factory(payload),
    )

    records = list(fetcher.fetch_all())
    assert len(records) == 2
    assert records[0].source == "crossref"


def test_streams_raw_pages(fake_response_factory, monkeypatch) -> None:
    payload = {"message": {"items": [_make_item(DOI="a"), _make_item(DOI="b")]}}
    fetcher = CrossRefFetcher(CrossRefConfig(query="q", max_pages=1, rows=2))
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda url, params, timeout: fake_response_factory(payload),
    )

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert len(pages[0]) == 2
    assert pages[0][0]["DOI"] == "a"


def test_stops_on_empty_items(fake_response_factory, monkeypatch) -> None:
    payload = {"message": {"items": []}}
    config = CrossRefConfig(query="empty", max_pages=3, rows=2)
    fetcher = CrossRefFetcher(config)
    call_count = 0

    def fake_get(url, params, timeout):
        nonlocal call_count
        call_count += 1
        return fake_response_factory(payload)

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    assert list(fetcher.fetch_all()) == []
    assert call_count == 1


def test_advances_offset_between_pages(fake_response_factory, monkeypatch) -> None:
    offsets: list[int] = []

    def fake_get(url, params, timeout):
        offsets.append(params["offset"])
        if params["offset"] == 0:
            return fake_response_factory({"message": {"items": [_make_item()]}})
        return fake_response_factory({"message": {"items": []}})

    config = CrossRefConfig(query="q", max_pages=3, rows=1)
    fetcher = CrossRefFetcher(config)
    monkeypatch.setattr(fetcher.session, "get", fake_get)

    records = list(fetcher.fetch_all())
    assert len(records) == 1
    assert offsets == [0, 1]
