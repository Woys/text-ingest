from __future__ import annotations

import pytest
import requests

from data_ingestion.config import GoogleNewsConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.googlenews import GoogleNewsFetcher


class _Resp:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_normalize_maps_fields() -> None:
    fetcher = GoogleNewsFetcher(GoogleNewsConfig(query="ai", max_pages=1))
    rec = fetcher.normalize(
        {
            "guid": "g1",
            "title": "AI headline",
            "pubDate": "Tue, 10 Mar 2026 10:00:00 GMT",
            "link": "https://example.com/post",
            "description": "summary",
        }
    )
    assert rec.external_id == "g1"
    assert rec.title == "AI headline"
    assert rec.url == "https://example.com/post"
    assert rec.abstract == "summary"


def test_fetch_pages_success(monkeypatch) -> None:
    fetcher = GoogleNewsFetcher(
        GoogleNewsConfig(query="ai", hl="en-US", gl="US", ceid="US:en", page_size=2)
    )
    xml = """
    <rss>
      <channel>
        <language>en-US</language>
        <item>
          <title>A</title>
          <link>https://example.com/a</link>
          <guid>1</guid>
          <pubDate>Tue, 10 Mar 2026 10:00:00 GMT</pubDate>
          <description>desc a</description>
        </item>
        <item>
          <title>B</title>
          <link>https://example.com/b</link>
          <guid>2</guid>
          <pubDate>Tue, 10 Mar 2026 11:00:00 GMT</pubDate>
          <description>desc b</description>
        </item>
      </channel>
    </rss>
    """
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(xml))

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["guid"] for item in pages[0]] == ["1", "2"]
    assert all(item["language"] == "en-US" for item in pages[0])


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = GoogleNewsFetcher(GoogleNewsConfig(query="ai"))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="GoogleNews request failed"):
        list(fetcher.fetch_pages())


def test_fetch_pages_wraps_parse_error(monkeypatch) -> None:
    fetcher = GoogleNewsFetcher(GoogleNewsConfig(query="ai"))
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp("<rss><broken>"))

    with pytest.raises(FetcherError, match="invalid XML"):
        list(fetcher.fetch_pages())
