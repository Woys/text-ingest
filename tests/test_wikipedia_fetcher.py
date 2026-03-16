from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import WikipediaConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.wikipedia import WikipediaFetcher


class _Resp:
    def __init__(self, payload=None, text: str = "") -> None:
        self._payload = payload
        self.text = text

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_normalize_maps_fields() -> None:
    fetcher = WikipediaFetcher(WikipediaConfig(query="cloud", max_pages=1))
    rec = fetcher.normalize(
        {
            "pageid": 123,
            "title": "Cloud computing",
            "timestamp": "2026-03-15T10:00:00Z",
            "extract": "Summary",
            "url": "https://en.wikipedia.org/?curid=123",
            "lang": "en",
        }
    )
    assert rec.external_id == "123"
    assert rec.title == "Cloud computing"
    assert rec.published_date == date(2026, 3, 15)


def test_fetch_pages_enriches_with_summary(monkeypatch) -> None:
    fetcher = WikipediaFetcher(WikipediaConfig(query="cloud", max_pages=1))

    search_payload = {"query": {"search": [{"title": "Cloud", "pageid": 1}]}}
    summary_payload = {
        "extract": "Cloud summary",
        "content_urls": {"desktop": {"page": "https://en.wikipedia.org/?curid=1"}},
    }

    def fake_get(url, **kwargs):
        del kwargs
        if "w/api.php" in url:
            return _Resp(payload=search_payload)
        return _Resp(payload=summary_payload)

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert pages[0][0]["extract"] == "Cloud summary"


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = WikipediaFetcher(WikipediaConfig(query="cloud", max_pages=1))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="Wikipedia request failed"):
        list(fetcher.fetch_pages())
