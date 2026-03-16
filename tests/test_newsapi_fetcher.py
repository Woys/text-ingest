"""Tests for NewsApiFetcher — pagination and normalization."""

from datetime import date

import pytest

from data_ingestion.config import NewsApiConfig
from data_ingestion.fetchers.newsapi import NewsApiFetcher
from data_ingestion.models import RecordType


def _make_article(**kwargs):
    base = {
        "title": "AI Breakthrough Announced",
        "author": "Jane Doe",
        "description": "Scientists have announced a new AI discovery.",
        "url": "https://example.com/news/ai-breakthrough",
        "publishedAt": "2026-03-14T10:30:00Z",
        "source": {"id": "bbc-news", "name": "BBC News"},
        "content": "Full article content here...",
    }
    base.update(kwargs)
    return base


def _ok_payload(articles, page_size=20):
    return {"status": "ok", "totalResults": len(articles), "articles": articles}


@pytest.fixture()
def fetcher(monkeypatch):
    monkeypatch.setenv("NEWSAPI_KEY", "test_key_123")
    return NewsApiFetcher(NewsApiConfig(query="AI", max_pages=1))


def test_normalize_all_fields(fetcher) -> None:
    article = _make_article()
    rec = fetcher.normalize(article)

    assert rec.source == "newsapi"
    assert rec.external_id == "https://example.com/news/ai-breakthrough"
    assert rec.title == "AI Breakthrough Announced"
    assert rec.authors == ["Jane Doe"]
    assert rec.published_date == date(2026, 3, 14)
    assert rec.url == "https://example.com/news/ai-breakthrough"
    assert rec.abstract == "Scientists have announced a new AI discovery."
    assert rec.topic == "BBC News"
    assert rec.record_type == RecordType.NEWS
    assert rec.raw_payload == article
    assert rec.external_id == "https://example.com/news/ai-breakthrough"
    assert rec.full_text == "Full article content here..."
    assert rec.full_text_url == "https://example.com/news/ai-breakthrough"


def test_normalize_missing_author(fetcher) -> None:
    article = _make_article(author=None)
    rec = fetcher.normalize(article)
    assert rec.authors == []


def test_normalize_blank_author(fetcher) -> None:
    article = _make_article(author="   ")
    rec = fetcher.normalize(article)
    assert rec.authors == []


def test_normalize_missing_date(fetcher) -> None:
    article = _make_article(publishedAt=None)
    rec = fetcher.normalize(article)
    assert rec.published_date is None


def test_normalize_invalid_date(fetcher) -> None:
    article = _make_article(publishedAt="not-a-date")
    rec = fetcher.normalize(article)
    assert rec.published_date is None


def test_normalize_record_type_is_always_news(fetcher) -> None:
    rec = fetcher.normalize(_make_article())
    assert rec.record_type == RecordType.NEWS


def test_streams_articles(fake_response_factory, monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    articles = [_make_article(url=f"https://ex.com/{i}") for i in range(3)]
    payload = _ok_payload(articles)

    config = NewsApiConfig(query="AI", max_pages=1, page_size=20)
    fetcher = NewsApiFetcher(config)
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda url, params, timeout: fake_response_factory(payload),
    )

    records = list(fetcher.fetch_all())
    assert len(records) == 3
    assert records[0].source == "newsapi"
    assert records[0].record_type == RecordType.NEWS


def test_streams_raw_pages(fake_response_factory, monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    articles = [_make_article(url=f"https://ex.com/{i}") for i in range(3)]
    payload = _ok_payload(articles)

    fetcher = NewsApiFetcher(NewsApiConfig(query="AI", max_pages=1, page_size=20))
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda url, params, timeout: fake_response_factory(payload),
    )

    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert len(pages[0]) == 3
    assert pages[0][0]["url"] == "https://ex.com/0"


def test_stops_on_empty_articles(fake_response_factory, monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    payload = _ok_payload([])

    config = NewsApiConfig(query="empty", max_pages=3, page_size=20)
    fetcher = NewsApiFetcher(config)
    call_count = 0

    def fake_get(url, params, timeout):
        nonlocal call_count
        call_count += 1
        return fake_response_factory(payload)

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    assert list(fetcher.fetch_all()) == []
    assert call_count == 1


def test_stops_on_partial_page(fake_response_factory, monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    articles = [_make_article(url=f"https://ex.com/{i}") for i in range(5)]
    payload = _ok_payload(articles)

    config = NewsApiConfig(query="q", max_pages=3, page_size=20)
    fetcher = NewsApiFetcher(config)
    call_count = 0

    def fake_get(url, params, timeout):
        nonlocal call_count
        call_count += 1
        return fake_response_factory(payload)

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    records = list(fetcher.fetch_all())
    assert len(records) == 5
    assert call_count == 1


def test_raises_on_api_error(fake_response_factory, monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "bad_key")
    payload = {
        "status": "error",
        "code": "apiKeyInvalid",
        "message": "Your API key is invalid.",
    }

    config = NewsApiConfig(query="q", api_key="bad_key", max_pages=1)
    fetcher = NewsApiFetcher(config)
    monkeypatch.setattr(
        fetcher.session,
        "get",
        lambda url, params, timeout: fake_response_factory(payload),
    )

    from data_ingestion.exceptions import FetcherError

    with pytest.raises(FetcherError, match="apiKeyInvalid"):
        list(fetcher.fetch_all())


def test_fetch_pages_requests_all_configured_languages(monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    fetcher = NewsApiFetcher(
        NewsApiConfig(
            query="AI",
            max_pages=1,
            page_size=20,
            languages=["en", "fr"],
        )
    )

    called_languages: list[str] = []

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    def fake_get(url, params, timeout):
        del url, timeout
        called_languages.append(params["language"])
        article = _make_article(url=f"https://example.com/{params['language']}")
        return _Resp({"status": "ok", "totalResults": 1, "articles": [article]})

    monkeypatch.setattr(fetcher.session, "get", fake_get)
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 2
    assert called_languages == ["en", "fr"]
