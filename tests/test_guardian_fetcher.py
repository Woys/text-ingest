from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import GuardianConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.guardian import GuardianFetcher


class _Resp:
    def __init__(self, payload=None, raise_json: bool = False) -> None:
        self._payload = payload
        self._raise_json = raise_json

    def raise_for_status(self) -> None:
        return None

    def json(self):
        if self._raise_json:
            raise ValueError("bad json")
        return self._payload


def test_normalize_maps_fields() -> None:
    fetcher = GuardianFetcher(GuardianConfig(query="ai", max_pages=1))
    rec = fetcher.normalize(
        {
            "id": "world/2026/mar/10/example",
            "webTitle": "Title",
            "webPublicationDate": "2026-03-10T10:00:00Z",
            "webUrl": "https://theguardian.com/example",
            "sectionName": "World news",
        }
    )
    assert rec.external_id == "world/2026/mar/10/example"
    assert rec.title == "Title"
    assert rec.topic == "World news"
    assert rec.url == "https://theguardian.com/example"


def test_fetch_pages_success(monkeypatch) -> None:
    fetcher = GuardianFetcher(
        GuardianConfig(
            query="ai",
            max_pages=2,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
        )
    )
    calls: list[dict[str, object]] = []

    def fake_get(*a, **k):
        del a
        params = k.get("params")
        assert isinstance(params, dict)
        calls.append(params)
        return _Resp(
            {
                "response": {
                    "status": "ok",
                    "pages": 1,
                    "results": [
                        {
                            "id": "one",
                            "webTitle": "One",
                            "webPublicationDate": "2026-03-10T10:00:00Z",
                            "webUrl": "https://theguardian.com/one",
                            "sectionName": "Technology",
                        }
                    ],
                }
            }
        )

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert pages[0][0]["id"] == "one"
    assert pages[0][0]["lang"] == "en"
    assert calls[0]["from-date"] == "2026-03-01"
    assert calls[0]["to-date"] == "2026-03-31"


def test_fetch_pages_status_error(monkeypatch) -> None:
    fetcher = GuardianFetcher(GuardianConfig(query="ai", max_pages=1))
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda *a, **k: _Resp({"response": {"status": "error", "message": "bad"}}),
    )
    with pytest.raises(FetcherError, match="Guardian error"):
        list(fetcher.fetch_pages())


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = GuardianFetcher(GuardianConfig(query="ai", max_pages=1))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="Guardian request failed"):
        list(fetcher.fetch_pages())


def test_fetch_pages_wraps_invalid_json(monkeypatch) -> None:
    fetcher = GuardianFetcher(GuardianConfig(query="ai", max_pages=1))
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(raise_json=True))

    with pytest.raises(FetcherError, match="invalid JSON"):
        list(fetcher.fetch_pages())
