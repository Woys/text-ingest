from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import EdgarConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.edgar import EdgarFetcher


class _Resp:
    def __init__(self, payload) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_normalize_maps_fields() -> None:
    fetcher = EdgarFetcher(EdgarConfig(query="data", max_pages=1))
    rec = fetcher.normalize(
        {
            "_id": "0001185185-15-000457:ex21-1.htm",
            "_source": {
                "ciks": ["0001418121"],
                "period_ending": "2014-12-31",
                "file_num": ["000-53603"],
                "display_names": [
                    "Apple Hospitality REIT, Inc.  (APLE)  (CIK 0001418121)"
                ],
                "root_forms": ["10-K"],
                "file_date": "2015-03-06",
            },
        }
    )
    assert rec.external_id == "0001185185-15-000457:ex21-1.htm"
    assert rec.title == "10-K - Apple Hospitality REIT, Inc.  (APLE)  (CIK 0001418121)"
    assert rec.published_date == date(2015, 3, 6)
    assert rec.authors == ["Apple Hospitality REIT, Inc.  (APLE)  (CIK 0001418121)"]
    assert (
        rec.full_text_url
        == "https://www.sec.gov/Archives/edgar/data/1418121/000118518515000457/ex21-1.htm"
    )


def test_fetch_pages_success(monkeypatch) -> None:
    fetcher = EdgarFetcher(EdgarConfig(query="data", max_pages=1, per_page=2))
    payload = {"hits": {"hits": [{"_id": "1"}, {"_id": "2"}]}}
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["_id"] for item in pages[0]] == ["1", "2"]


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = EdgarFetcher(EdgarConfig(query="data", max_pages=1))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="Edgar request failed"):
        list(fetcher.fetch_pages())


def test_normalize_handles_missing_fields() -> None:
    fetcher = EdgarFetcher(EdgarConfig(query="data", max_pages=1))
    rec = fetcher.normalize(
        {
            "_id": "invalid-id",
            "_source": {
                "file_date": "invalid-date",
            },
        }
    )
    assert rec.external_id == "invalid-id"
    assert rec.published_date is None
    assert rec.full_text_url is None
    assert rec.title == "Filing"
    assert rec.authors == []


def test_extract_language() -> None:
    fetcher = EdgarFetcher(EdgarConfig(query="data", max_pages=1))
    assert fetcher.extract_language({}) == "en"


def test_fetch_pages_with_dates(monkeypatch) -> None:
    fetcher = EdgarFetcher(
        EdgarConfig(
            query="data",
            max_pages=1,
            per_page=2,
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
        )
    )
    payload = {
        "hits": {"hits": [{"_id": "1"}]}
    }  # Length < per_page to test early return

    def mock_get(url, params, **kwargs):
        assert params["startdt"] == "2023-01-01"
        assert params["enddt"] == "2023-12-31"
        return _Resp(payload)

    monkeypatch.setattr(fetcher.session, "get", mock_get)
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1


def test_fetch_pages_empty_hits(monkeypatch) -> None:
    fetcher = EdgarFetcher(EdgarConfig(query="data", max_pages=1))
    payload = {"hits": {"hits": []}}
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 0


class _RespInvalidJson:
    def raise_for_status(self) -> None:
        pass

    def json(self):
        raise ValueError("Invalid JSON")


def test_fetch_pages_invalid_json(monkeypatch) -> None:
    fetcher = EdgarFetcher(EdgarConfig(query="data", max_pages=1))
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _RespInvalidJson())
    with pytest.raises(FetcherError, match="invalid JSON"):
        list(fetcher.fetch_pages())


def test_parse_date_none() -> None:
    assert EdgarFetcher._parse_date(None) is None
