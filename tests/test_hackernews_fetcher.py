"""Tests for HackerNewsFetcher — pagination and normalization."""

from datetime import date

import pytest
import requests

from data_ingestion.config import HackerNewsConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.hackernews import HackerNewsFetcher
from data_ingestion.models import RecordType


def _make_item(**kwargs):
    base = {
        "objectID": "123",
        "title": "Show HN: Data Tool",
        "story_title": None,
        "author": "alice",
        "created_at": "2026-03-15T10:00:00Z",
        "url": "https://example.com/post",
        "story_text": "full story",
        "_tags": ["story", "show_hn", "author_alice", "story_123"],
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
    return HackerNewsFetcher(HackerNewsConfig(query="data", max_pages=1))


def test_parse_date_and_extract_topic(fetcher) -> None:
    assert fetcher._parse_date("2026-03-15T10:00:00Z") == date(2026, 3, 15)
    assert fetcher._parse_date("not-a-date") is None

    # The first non-ignored tag in default test payload is "story".
    assert fetcher._extract_topic(_make_item()) == "story"


def test_normalize_populates_fields(fetcher) -> None:
    rec = fetcher.normalize(_make_item())

    assert rec.source == "hackernews"
    assert rec.external_id == "123"
    assert rec.title == "Show HN: Data Tool"
    assert rec.authors == ["alice"]
    assert rec.published_date == date(2026, 3, 15)
    assert rec.url == "https://example.com/post"
    assert rec.full_text == "full story"
    assert rec.full_text_url == "https://example.com/post"
    assert rec.record_type == RecordType.NEWS


def test_normalize_fallback_url(fetcher) -> None:
    rec = fetcher.normalize(_make_item(url=None, objectID="999"))
    assert rec.url == "https://news.ycombinator.com/item?id=999"


def test_fetch_pages_yields_hits_and_stops(monkeypatch) -> None:
    payload = {"hits": [_make_item(objectID="1")], "nbPages": 1}
    fetcher = HackerNewsFetcher(
        HackerNewsConfig(query="x", max_pages=3, hits_per_page=5)
    )
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert pages[0][0]["objectID"] == "1"


def test_fetch_pages_stops_on_empty_hits(monkeypatch) -> None:
    payload = {"hits": [], "nbPages": 10}
    fetcher = HackerNewsFetcher(
        HackerNewsConfig(query="x", max_pages=3, hits_per_page=5)
    )
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))

    assert list(fetcher.fetch_pages()) == []


def test_fetch_pages_wraps_request_exception(monkeypatch) -> None:
    fetcher = HackerNewsFetcher(HackerNewsConfig(query="x", max_pages=1))

    def boom(*a, **k):
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)

    with pytest.raises(FetcherError, match="request failed"):
        list(fetcher.fetch_pages())


def test_fetch_pages_wraps_invalid_json(monkeypatch) -> None:
    fetcher = HackerNewsFetcher(HackerNewsConfig(query="x", max_pages=1))

    class _BadResp:
        def raise_for_status(self):
            return None

        def json(self):
            raise ValueError("bad json")

    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _BadResp())

    with pytest.raises(FetcherError, match="invalid JSON"):
        list(fetcher.fetch_pages())
