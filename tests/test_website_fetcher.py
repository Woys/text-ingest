"""Tests for WebsiteFetcher — feed autodiscovery, parsing, and filtering."""

from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import WebsiteConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.website import WebsiteFetcher
from data_ingestion.models import RecordType


class _Resp:
    def __init__(self, *, text: str, url: str) -> None:
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        return None


def _rss_feed(*, items: str, title: str = "AWS News Blog") -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:content="http://purl.org/rss/1.0/modules/content/"
     xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>{title}</title>
    {items}
  </channel>
</rss>
"""


def _rss_item(
    *,
    title: str,
    link: str,
    guid: str,
    pub_date: str,
    description: str,
    content: str,
    author: str = "Jane Doe",
    category: str = "News",
) -> str:
    return f"""
    <item>
      <title>{title}</title>
      <link>{link}</link>
      <guid>{guid}</guid>
      <dc:creator><![CDATA[{author}]]></dc:creator>
      <pubDate>{pub_date}</pubDate>
      <category>{category}</category>
      <description><![CDATA[{description}]]></description>
      <content:encoded><![CDATA[{content}]]></content:encoded>
    </item>
"""


def test_fetch_pages_autodiscovers_feed_from_site(monkeypatch) -> None:
    fetcher = WebsiteFetcher(
        WebsiteConfig(site_url="https://aws.amazon.com/blogs/aws/", max_items=10)
    )

    site_html = """
    <html><head>
      <link rel="alternate" type="application/rss+xml" href="/blogs/aws/feed/" />
    </head></html>
    """
    feed_xml = _rss_feed(
        items=_rss_item(
            title="S3 launch",
            link="https://aws.amazon.com/blogs/aws/s3-launch/",
            guid="guid-1",
            pub_date="Fri, 13 Mar 2026 12:58:39 +0000",
            description="<p>Short <b>summary</b></p>",
            content="<p>Long <i>content</i> body</p>",
        )
    )

    def fake_get(url: str, **kwargs) -> _Resp:
        del kwargs
        if url == "https://aws.amazon.com/blogs/aws/":
            return _Resp(text=site_html, url=url)
        if url == "https://aws.amazon.com/blogs/aws/feed/":
            return _Resp(text=feed_xml, url=url)
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(fetcher.session, "get", fake_get)

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert len(pages[0]) == 1
    assert pages[0][0]["feed_title"] == "AWS News Blog"
    assert pages[0][0]["link"] == "https://aws.amazon.com/blogs/aws/s3-launch/"


def test_fetch_pages_uses_feed_url_directly(monkeypatch) -> None:
    fetcher = WebsiteFetcher(
        WebsiteConfig(feed_url="https://example.com/feed.xml", max_items=10)
    )
    feed_xml = _rss_feed(
        items=_rss_item(
            title="Direct feed entry",
            link="https://example.com/post/1",
            guid="post-1",
            pub_date="2026-03-11T00:00:00Z",
            description="Desc",
            content="<p>Body</p>",
        )
    )

    called_urls: list[str] = []

    def fake_get(url: str, **kwargs) -> _Resp:
        del kwargs
        called_urls.append(url)
        return _Resp(text=feed_xml, url=url)

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert called_urls == ["https://example.com/feed.xml"]


def test_normalize_maps_fields_and_strips_html() -> None:
    fetcher = WebsiteFetcher(
        WebsiteConfig(feed_url="https://example.com/feed.xml", max_items=10)
    )
    item = {
        "title": "<b>Big launch</b>",
        "link": "https://example.com/posts/1",
        "guid": "guid-123",
        "author": "  Alice  ",
        "published": "Fri, 13 Mar 2026 12:58:39 +0000",
        "description": "<p>Small <i>summary</i>.</p>",
        "content": "<div>Full <strong>article</strong> text.</div>",
        "categories": ["Launch", "News"],
        "feed_title": "Example Feed",
    }

    rec = fetcher.normalize(item)
    assert rec.source == "website"
    assert rec.external_id == "guid-123"
    assert rec.title == "Big launch"
    assert rec.authors == ["Alice"]
    assert rec.published_date == date(2026, 3, 13)
    assert rec.url == "https://example.com/posts/1"
    assert rec.abstract == "Small summary."
    assert rec.full_text == "Full article text."
    assert rec.full_text_url == "https://example.com/posts/1"
    assert rec.topic == "Launch"
    assert rec.record_type == RecordType.NEWS


def test_fetch_pages_applies_optional_query_filter(monkeypatch) -> None:
    fetcher = WebsiteFetcher(
        WebsiteConfig(
            feed_url="https://example.com/feed.xml",
            query="s3",
            search_mode="broad",
            max_items=10,
        )
    )
    feed_xml = _rss_feed(
        items="".join(
            [
                _rss_item(
                    title="S3 update",
                    link="https://example.com/1",
                    guid="1",
                    pub_date="2026-03-12T00:00:00Z",
                    description="Storage update",
                    content="<p>content one</p>",
                ),
                _rss_item(
                    title="EC2 update",
                    link="https://example.com/2",
                    guid="2",
                    pub_date="2026-03-12T00:00:00Z",
                    description="Compute update",
                    content="<p>content two</p>",
                ),
            ]
        )
    )
    monkeypatch.setattr(
        fetcher.session, "get", lambda *a, **k: _Resp(text=feed_xml, url=a[0])
    )

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["guid"] for item in pages[0]] == ["1"]


def test_fetch_pages_applies_date_range_and_max_items(monkeypatch) -> None:
    fetcher = WebsiteFetcher(
        WebsiteConfig(
            feed_url="https://example.com/feed.xml",
            start_date=date(2026, 3, 11),
            end_date=date(2026, 3, 12),
            max_items=1,
        )
    )
    feed_xml = _rss_feed(
        items="".join(
            [
                _rss_item(
                    title="Old",
                    link="https://example.com/old",
                    guid="old",
                    pub_date="2026-03-09T00:00:00Z",
                    description="old",
                    content="<p>old</p>",
                ),
                _rss_item(
                    title="In range",
                    link="https://example.com/newer",
                    guid="newer",
                    pub_date="2026-03-12T00:00:00Z",
                    description="newer",
                    content="<p>newer</p>",
                ),
                _rss_item(
                    title="Also in range",
                    link="https://example.com/newest",
                    guid="newest",
                    pub_date="2026-03-12T01:00:00Z",
                    description="newest",
                    content="<p>newest</p>",
                ),
            ]
        )
    )
    monkeypatch.setattr(
        fetcher.session, "get", lambda *a, **k: _Resp(text=feed_xml, url=a[0])
    )

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert len(pages[0]) == 1
    assert pages[0][0]["guid"] == "newer"


def test_fetch_pages_applies_target_date_filter(monkeypatch) -> None:
    fetcher = WebsiteFetcher(
        WebsiteConfig(
            feed_url="https://example.com/feed.xml",
            target_date=date(2026, 3, 12),
            max_items=10,
        )
    )
    feed_xml = _rss_feed(
        items="".join(
            [
                _rss_item(
                    title="March 11 post",
                    link="https://example.com/11",
                    guid="11",
                    pub_date="2026-03-11T00:00:00Z",
                    description="older",
                    content="<p>older</p>",
                ),
                _rss_item(
                    title="March 12 post A",
                    link="https://example.com/12a",
                    guid="12a",
                    pub_date="2026-03-12T00:00:00Z",
                    description="same day",
                    content="<p>same day</p>",
                ),
                _rss_item(
                    title="March 12 post B",
                    link="https://example.com/12b",
                    guid="12b",
                    pub_date="Fri, 12 Mar 2026 22:00:00 +0000",
                    description="same day too",
                    content="<p>same day too</p>",
                ),
            ]
        )
    )
    monkeypatch.setattr(
        fetcher.session, "get", lambda *a, **k: _Resp(text=feed_xml, url=a[0])
    )

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["guid"] for item in pages[0]] == ["12a", "12b"]


def test_fetch_pages_wraps_network_errors(monkeypatch) -> None:
    fetcher = WebsiteFetcher(WebsiteConfig(feed_url="https://example.com/feed.xml"))

    def boom(*args, **kwargs) -> _Resp:
        del args, kwargs
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)

    with pytest.raises(FetcherError, match="request failed"):
        list(fetcher.fetch_pages())


def test_fetch_pages_raises_on_malformed_xml(monkeypatch) -> None:
    fetcher = WebsiteFetcher(WebsiteConfig(feed_url="https://example.com/feed.xml"))
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda *a, **k: _Resp(text="<rss><channel><item></rss>", url=a[0]),
    )

    with pytest.raises(FetcherError, match="invalid XML"):
        list(fetcher.fetch_pages())
