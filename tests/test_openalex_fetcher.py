"""Tests for OpenAlexFetcher — pagination and normalization."""

from datetime import date

import pytest

from data_ingestion.config import OpenAlexConfig
from data_ingestion.fetchers.openalex import OpenAlexFetcher
from data_ingestion.models import RecordType


def _make_work(**kwargs):
    base = {
        "id": "https://openalex.org/W1",
        "title": "Test Paper",
        "publication_date": "2024-03-01",
        "authorships": [
            {"author": {"display_name": "Alice Smith"}},
            {"author": {"display_name": "Bob Jones"}},
        ],
        "doi": "https://doi.org/10.1000/test",
        "abstract_inverted_index": {"Hello": [0], "world": [1]},
    }
    base.update(kwargs)
    return base


@pytest.fixture()
def fetcher():
    return OpenAlexFetcher(OpenAlexConfig(query="test", max_pages=1))


def test_normalize_populates_all_fields(fetcher) -> None:
    work = _make_work(primary_topic={"display_name": "Cancer Biology"})
    rec = fetcher.normalize(work)

    assert rec.source == "openalex"
    assert rec.external_id == "https://openalex.org/W1"
    assert rec.title == "Test Paper"
    assert rec.authors == ["Alice Smith", "Bob Jones"]
    assert rec.published_date == date(2024, 3, 1)
    assert rec.url == "https://doi.org/10.1000/test"
    assert rec.abstract == "Hello world"
    assert rec.topic == "Cancer Biology"
    assert rec.record_type == RecordType.ARTICLE
    assert rec.raw_payload == work


def test_normalize_reconstructs_abstract(fetcher) -> None:
    work = _make_work(abstract_inverted_index={"Quick": [0], "brown": [1], "fox": [2]})
    rec = fetcher.normalize(work)
    assert rec.abstract == "Quick brown fox"


def test_normalize_topic_falls_back_to_concepts(fetcher) -> None:
    work = _make_work(
        primary_topic=None,
        concepts=[{"display_name": "Oncology"}, {"display_name": "Medicine"}],
    )
    rec = fetcher.normalize(work)
    assert rec.topic == "Oncology"


def test_normalize_missing_abstract(fetcher) -> None:
    work = _make_work(abstract_inverted_index=None)
    del work["abstract_inverted_index"]
    rec = fetcher.normalize(work)
    assert rec.abstract is None


def test_normalize_invalid_date(fetcher) -> None:
    work = _make_work(publication_date="not-a-date")
    rec = fetcher.normalize(work)
    assert rec.published_date is None


def test_normalize_falls_back_to_id_when_no_doi(fetcher) -> None:
    work = _make_work()
    del work["doi"]
    rec = fetcher.normalize(work)
    assert rec.url == "https://openalex.org/W1"


def test_streams_records(fake_response_factory, monkeypatch) -> None:
    payload = {
        "results": [_make_work(id="W1"), _make_work(id="W2")],
        "meta": {"next_cursor": None},
    }
    config = OpenAlexConfig(query="cancer prevention", max_pages=1, per_page=2)
    fetcher = OpenAlexFetcher(config)
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda url, params, timeout: fake_response_factory(payload),
    )

    records = list(fetcher.fetch_all())
    assert len(records) == 2
    assert records[0].source == "openalex"


def test_streams_raw_pages(fake_response_factory, monkeypatch) -> None:
    payload = {
        "results": [_make_work(id="W1"), _make_work(id="W2")],
        "meta": {"next_cursor": None},
    }
    fetcher = OpenAlexFetcher(OpenAlexConfig(query="q", max_pages=1, per_page=2))
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda url, params, timeout: fake_response_factory(payload),
    )

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert len(pages[0]) == 2
    assert pages[0][0]["id"] == "W1"


def test_stops_on_empty_results(fake_response_factory, monkeypatch) -> None:
    payload = {"results": [], "meta": {"next_cursor": None}}
    config = OpenAlexConfig(query="empty", max_pages=3, per_page=2)
    fetcher = OpenAlexFetcher(config)
    call_count = 0

    def fake_get(url, params, timeout):
        nonlocal call_count
        call_count += 1
        return fake_response_factory(payload)

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    assert list(fetcher.fetch_all()) == []
    assert call_count == 1


def test_follows_cursor_pagination(fake_response_factory, monkeypatch) -> None:
    pages = [
        {"results": [_make_work(id="W1")], "meta": {"next_cursor": "cursor-2"}},
        {"results": [_make_work(id="W2")], "meta": {"next_cursor": None}},
    ]
    page_iter = iter(pages)
    config = OpenAlexConfig(query="q", max_pages=5, per_page=1)
    fetcher = OpenAlexFetcher(config)
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda u, params, timeout: fake_response_factory(next(page_iter)),
    )
    assert len(list(fetcher.fetch_all())) == 2
