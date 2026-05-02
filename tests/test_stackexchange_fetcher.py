from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import StackExchangeConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.stackexchange import StackExchangeFetcher


class _Resp:
    def __init__(self, payload) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_normalize_maps_fields() -> None:
    fetcher = StackExchangeFetcher(StackExchangeConfig(query="python", max_pages=1))
    rec = fetcher.normalize(
        {
            "question_id": 123,
            "title": "How to test",
            "creation_date": 1760000000,
            "link": "https://stackoverflow.com/q/123",
            "tags": ["python", "pytest"],
            "owner": {"display_name": "alice"},
        }
    )
    assert rec.external_id == "123"
    assert rec.title == "How to test"
    assert rec.authors == ["alice"]
    assert rec.topic == "python"
    assert isinstance(rec.published_date, date)


def test_fetch_pages_success(monkeypatch) -> None:
    fetcher = StackExchangeFetcher(
        StackExchangeConfig(query="python", max_pages=1, page_size=2)
    )
    payload = {"items": [{"question_id": 1}, {"question_id": 2}], "has_more": False}
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["question_id"] for item in pages[0]] == [1, 2]


def test_fetch_pages_sends_date_range_params(monkeypatch) -> None:
    fetcher = StackExchangeFetcher(
        StackExchangeConfig(
            query="python",
            max_pages=1,
            start_date=date(2026, 3, 10),
            end_date=date(2026, 3, 10),
        )
    )
    payload = {"items": [{"question_id": 1}], "has_more": False}

    def mock_get(url, params, **kwargs):
        del url, kwargs
        assert params["fromdate"] == 1773100800
        assert params["todate"] == 1773187199
        return _Resp(payload)

    monkeypatch.setattr(fetcher.session, "get", mock_get)
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = StackExchangeFetcher(StackExchangeConfig(query="python", max_pages=1))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="StackExchange request failed"):
        list(fetcher.fetch_pages())
