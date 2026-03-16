"""Tests for WebsiteHtmlFetcher."""

from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import WebsiteHtmlConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.website_html import WebsiteHtmlFetcher
from data_ingestion.models import RecordType


class _Resp:
    def __init__(self, *, text: str, url: str) -> None:
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        return None


def test_fetch_pages_extracts_links_and_applies_filters(monkeypatch) -> None:
    fetcher = WebsiteHtmlFetcher(
        WebsiteHtmlConfig(
            site_url="https://example.com",
            list_page_urls=["https://example.com/news"],
            link_include_patterns=["/post/"],
            link_exclude_patterns=["/ignore/"],
            query="ai",
            search_mode="broad",
            start_date=date(2026, 3, 10),
            end_date=date(2026, 3, 10),
            max_items=10,
        )
    )

    list_html = """
    <a href="/post/1">Post 1</a>
    <a href="/post/2">Post 2</a>
    <a href="/post/ignore/3">Post 3</a>
    <a href="https://other.com/post/4">Other site</a>
    """
    article_1 = """
    <html><head>
      <meta property="og:title" content="AI launch" />
      <meta property="article:published_time" content="2026-03-10T14:00:00Z" />
      <meta name="description" content="AI content for launch" />
    </head><body><article>AI content for launch details.</article></body></html>
    """
    article_2 = """
    <html><head>
      <meta property="og:title" content="AI old post" />
      <meta property="article:published_time" content="2026-03-09T14:00:00Z" />
      <meta name="description" content="AI content old" />
    </head><body><article>AI content old details.</article></body></html>
    """

    def fake_get(url: str, **kwargs) -> _Resp:
        del kwargs
        if url == "https://example.com/news":
            return _Resp(text=list_html, url=url)
        if url == "https://example.com/post/1":
            return _Resp(text=article_1, url=url)
        if url == "https://example.com/post/2":
            return _Resp(text=article_2, url=url)
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(fetcher.session, "get", fake_get)

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert len(pages[0]) == 1
    assert pages[0][0]["url"] == "https://example.com/post/1"
    assert pages[0][0]["title"] == "AI launch"


def test_fetch_pages_returns_empty_when_no_links(monkeypatch) -> None:
    fetcher = WebsiteHtmlFetcher(
        WebsiteHtmlConfig(
            site_url="https://example.com",
            list_page_urls=["/news"],
            include_list_pages_as_items=False,
        )
    )
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda *a, **k: _Resp(text="<html><body>No links</body></html>", url=a[0]),
    )

    assert list(fetcher.fetch_pages()) == []


def test_fetch_pages_wraps_list_request_errors(monkeypatch) -> None:
    fetcher = WebsiteHtmlFetcher(WebsiteHtmlConfig(site_url="https://example.com"))

    def boom(*args, **kwargs) -> _Resp:
        del args, kwargs
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="list-page request failed"):
        list(fetcher.fetch_pages())


def test_normalize_maps_fields() -> None:
    fetcher = WebsiteHtmlFetcher(WebsiteHtmlConfig(site_url="https://example.com"))
    item = {
        "url": "https://example.com/post/123",
        "title": "New release",
        "published_raw": "2026-03-12T10:00:00Z",
        "summary": "Summary text",
        "content": "Long content text",
    }

    rec = fetcher.normalize(item)
    assert rec.source == "website_html"
    assert rec.external_id == "https://example.com/post/123"
    assert rec.title == "New release"
    assert rec.published_date == date(2026, 3, 12)
    assert rec.abstract == "Summary text"
    assert rec.full_text == "Long content text"
    assert rec.record_type == RecordType.NEWS
