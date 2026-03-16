from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import OpenLibraryConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.openlibrary import OpenLibraryFetcher


class _Resp:
    def __init__(self, payload) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_normalize_maps_fields() -> None:
    fetcher = OpenLibraryFetcher(OpenLibraryConfig(query="ai", max_pages=1))
    rec = fetcher.normalize(
        {
            "key": "/works/W1",
            "title": "AI book",
            "author_name": ["Alice"],
            "first_publish_year": 2020,
            "subject": ["ai"],
            "language": ["eng"],
        }
    )
    assert rec.external_id == "/works/W1"
    assert rec.title == "AI book"
    assert rec.authors == ["Alice"]
    assert rec.published_date == date(2020, 1, 1)


def test_fetch_pages_success(monkeypatch) -> None:
    fetcher = OpenLibraryFetcher(
        OpenLibraryConfig(query="ai", max_pages=1, page_size=2)
    )
    payload = {"docs": [{"key": "/w1"}, {"key": "/w2"}]}
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["key"] for item in pages[0]] == ["/w1", "/w2"]


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = OpenLibraryFetcher(OpenLibraryConfig(query="ai", max_pages=1))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="OpenLibrary request failed"):
        list(fetcher.fetch_pages())
