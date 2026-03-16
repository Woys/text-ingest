from __future__ import annotations

import pytest
import requests

from data_ingestion.config import RedditConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.reddit import RedditFetcher


class _Resp:
    def __init__(self, payload) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_normalize_maps_fields() -> None:
    fetcher = RedditFetcher(RedditConfig(query="ai", max_pages=1))
    rec = fetcher.normalize(
        {
            "id": "abc",
            "title": "AI trend",
            "author": "alice",
            "created_utc": 1760000000,
            "permalink": "/r/test/comments/abc/ai_trend/",
            "selftext": "Body",
            "subreddit": "test",
        }
    )
    assert rec.external_id == "abc"
    assert rec.authors == ["alice"]
    assert rec.topic == "test"
    assert rec.url and rec.url.startswith("https://www.reddit.com/")


def test_fetch_pages_success(monkeypatch) -> None:
    fetcher = RedditFetcher(RedditConfig(query="ai", max_pages=1, page_size=2))
    payload = {
        "data": {
            "children": [
                {"data": {"id": "1", "title": "x"}},
                {"data": {"id": "2", "title": "y"}},
            ],
            "after": None,
        }
    }
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["id"] for item in pages[0]] == ["1", "2"]


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = RedditFetcher(RedditConfig(query="ai", max_pages=1))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="Reddit request failed"):
        list(fetcher.fetch_pages())
