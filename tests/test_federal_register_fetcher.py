"""Tests for FederalRegisterFetcher — pagination and normalization."""

from datetime import date

import pytest

from data_ingestion.config import FederalRegisterConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.federal_register import FederalRegisterFetcher
from data_ingestion.models import RecordType


def _make_item(**kwargs):
    base = {
        "document_number": "FR-1",
        "title": "Federal Notice",
        "publication_date": "2026-03-14",
        "html_url": "https://federalregister.gov/doc/1",
        "pdf_url": "https://federalregister.gov/doc/1.pdf",
        "raw_text_url": "https://federalregister.gov/doc/1.txt",
        "abstract": "Notice abstract",
        "agencies": [{"raw_name": "EPA"}],
        "type": "Notice",
    }
    base.update(kwargs)
    return base


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@pytest.fixture()
def fetcher():
    return FederalRegisterFetcher(FederalRegisterConfig(query="data", max_pages=1))


def test_extract_topic_and_normalize(fetcher) -> None:
    rec = fetcher.normalize(_make_item())

    assert rec.source == "federalregister"
    assert rec.external_id == "FR-1"
    assert rec.title == "Federal Notice"
    assert rec.authors == ["EPA"]
    assert rec.published_date == date(2026, 3, 14)
    assert rec.url == "https://federalregister.gov/doc/1"
    assert rec.full_text_url == "https://federalregister.gov/doc/1.txt"
    assert rec.topic == "EPA"
    assert rec.record_type == RecordType.ARTICLE


def test_normalize_topic_fallback_to_type(fetcher) -> None:
    rec = fetcher.normalize(_make_item(agencies=[], type="Rule"))
    assert rec.topic == "Rule"


def test_normalize_invalid_date(fetcher) -> None:
    rec = fetcher.normalize(_make_item(publication_date="not-a-date"))
    assert rec.published_date is None


def test_fetch_pages_yields_and_stops(monkeypatch) -> None:
    payload = {"results": [_make_item(document_number="1")], "total_pages": 1}
    f = FederalRegisterFetcher(
        FederalRegisterConfig(query="x", max_pages=5, per_page=2)
    )
    monkeypatch.setattr(f.session, "get", lambda *a, **k: _Resp(payload))

    pages = list(f.fetch_pages())
    assert len(pages) == 1
    assert pages[0][0]["document_number"] == "1"


def test_fetch_pages_stops_on_empty_results(monkeypatch) -> None:
    payload = {"results": [], "total_pages": 10}
    f = FederalRegisterFetcher(
        FederalRegisterConfig(query="x", max_pages=5, per_page=2)
    )
    monkeypatch.setattr(f.session, "get", lambda *a, **k: _Resp(payload))

    assert list(f.fetch_pages()) == []


def test_fetch_pages_wraps_exceptions(monkeypatch) -> None:
    f = FederalRegisterFetcher(FederalRegisterConfig(query="x", max_pages=1))

    def boom(*a, **k):
        raise RuntimeError("bad request")

    monkeypatch.setattr(f.session, "get", boom)

    with pytest.raises(FetcherError, match="FederalRegister fail"):
        list(f.fetch_pages())
