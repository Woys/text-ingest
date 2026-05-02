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


def test_normalize_bad_date(fetcher) -> None:
    item = _make_item()
    item["published"] = {"date-parts": [["invalid", "date"]]}
    rec = fetcher.normalize(item)
    assert rec.published_date is None


def test_normalize_no_subject(fetcher) -> None:
    item = _make_item()
    item.pop("subject", None)
    rec = fetcher.normalize(item)
    assert rec.topic is None


def test_normalize_ft_url_intended_application(fetcher) -> None:
    item = _make_item()
    item["link"] = [
        {"URL": "http://fallback.com"},
        {"URL": "http://ft.com", "intended-application": "text-mining"},
    ]
    rec = fetcher.normalize(item)
    assert rec.full_text_url == "http://ft.com"


def test_extract_language(fetcher) -> None:
    assert fetcher.extract_language({"language": "en"}) == "en"
    assert fetcher.extract_language({}) is None


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


def test_fetch_pages_publication_date_mode(fake_response_factory, monkeypatch) -> None:
    config = CrossRefConfig(
        query="q",
        max_pages=1,
        rows=1,
        start_date=date(2023, 1, 1),
        end_date=date(2023, 1, 2),
        date_mode="publication",
    )
    config.http.email = "test@example.com"
    fetcher = CrossRefFetcher(config)

    def fake_get(url, params, timeout):
        assert params["filter"] == "from-pub-date:2023-01-01,until-pub-date:2023-01-02"
        assert params["query"] == "q"
        assert params["mailto"] == "test@example.com"
        return fake_response_factory({"message": {"items": [_make_item()]}})

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    assert len(list(fetcher.fetch_all())) == 1


def test_fetch_pages_update_date_mode(fake_response_factory, monkeypatch) -> None:
    config = CrossRefConfig(
        max_pages=1,
        rows=1,
        start_date=date(2023, 1, 1),
        end_date=date(2023, 1, 2),
        date_mode="update",
    )
    fetcher = CrossRefFetcher(config)

    def fake_get(url, params, timeout):
        assert (
            params["filter"]
            == "from-update-date:2023-01-01,until-update-date:2023-01-02"
        )
        assert "query" not in params
        return fake_response_factory({"message": {"items": [_make_item()]}})

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    assert len(list(fetcher.fetch_all())) == 1
