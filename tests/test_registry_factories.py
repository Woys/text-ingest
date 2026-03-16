import pytest
from pydantic import ValidationError

from data_ingestion.factories import build_fetcher, build_fetchers
from data_ingestion.fetchers.crossref import CrossRefFetcher
from data_ingestion.fetchers.github import GitHubFetcher
from data_ingestion.fetchers.googlenews import GoogleNewsFetcher
from data_ingestion.fetchers.guardian import GuardianFetcher
from data_ingestion.fetchers.newsapi import NewsApiFetcher
from data_ingestion.fetchers.openalex import OpenAlexFetcher
from data_ingestion.fetchers.openlibrary import OpenLibraryFetcher
from data_ingestion.fetchers.reddit import RedditFetcher
from data_ingestion.fetchers.stackexchange import StackExchangeFetcher
from data_ingestion.fetchers.website import WebsiteFetcher
from data_ingestion.fetchers.website_html import WebsiteHtmlFetcher
from data_ingestion.fetchers.wikipedia import WikipediaFetcher
from data_ingestion.registry import list_fetchers


def test_registry_contains_all_builtin_fetchers() -> None:
    fetchers = list_fetchers()
    assert "openalex" in fetchers
    assert "crossref" in fetchers
    assert "newsapi" in fetchers
    assert "website" in fetchers
    assert "website_html" in fetchers
    assert "wikipedia" in fetchers
    assert "reddit" in fetchers
    assert "github" in fetchers
    assert "stackexchange" in fetchers
    assert "openlibrary" in fetchers
    assert "googlenews" in fetchers
    assert "guardian" in fetchers


def test_build_fetcher_openalex() -> None:
    f = build_fetcher(
        {"source": "openalex", "config": {"query": "cancer prevention", "max_pages": 1}}
    )
    assert isinstance(f, OpenAlexFetcher)


def test_build_fetcher_crossref() -> None:
    f = build_fetcher(
        {"source": "crossref", "config": {"query": "cancer prevention", "max_pages": 1}}
    )
    assert isinstance(f, CrossRefFetcher)


def test_build_fetcher_newsapi(monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    f = build_fetcher(
        {"source": "newsapi", "config": {"query": "AI news", "max_pages": 1}}
    )
    assert isinstance(f, NewsApiFetcher)


def test_build_fetchers_returns_all_items(monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "test_key")
    fetchers = build_fetchers(
        [
            {"source": "openalex", "config": {"query": "x", "max_pages": 1}},
            {"source": "crossref", "config": {"query": "y", "max_pages": 1}},
            {"source": "newsapi", "config": {"query": "z", "max_pages": 1}},
            {"source": "wikipedia", "config": {"query": "z", "max_pages": 1}},
            {"source": "reddit", "config": {"query": "z", "max_pages": 1}},
            {"source": "github", "config": {"query": "z", "max_pages": 1}},
            {"source": "stackexchange", "config": {"query": "z", "max_pages": 1}},
            {"source": "openlibrary", "config": {"query": "z", "max_pages": 1}},
            {"source": "googlenews", "config": {"query": "z", "max_pages": 1}},
            {"source": "guardian", "config": {"query": "z", "max_pages": 1}},
            {"source": "website", "config": {"site_url": "https://example.com"}},
            {"source": "website_html", "config": {"site_url": "https://example.com"}},
        ]
    )
    assert len(fetchers) == 12


def test_build_fetcher_website() -> None:
    f = build_fetcher(
        {"source": "website", "config": {"feed_url": "https://example.com/feed.xml"}}
    )
    assert isinstance(f, WebsiteFetcher)


def test_build_fetcher_website_html() -> None:
    f = build_fetcher(
        {"source": "website_html", "config": {"site_url": "https://example.com"}}
    )
    assert isinstance(f, WebsiteHtmlFetcher)


def test_build_fetcher_wikipedia() -> None:
    f = build_fetcher({"source": "wikipedia", "config": {"query": "x"}})
    assert isinstance(f, WikipediaFetcher)


def test_build_fetcher_reddit() -> None:
    f = build_fetcher({"source": "reddit", "config": {"query": "x"}})
    assert isinstance(f, RedditFetcher)


def test_build_fetcher_github() -> None:
    f = build_fetcher({"source": "github", "config": {"query": "x"}})
    assert isinstance(f, GitHubFetcher)


def test_build_fetcher_stackexchange() -> None:
    f = build_fetcher({"source": "stackexchange", "config": {"query": "x"}})
    assert isinstance(f, StackExchangeFetcher)


def test_build_fetcher_openlibrary() -> None:
    f = build_fetcher({"source": "openlibrary", "config": {"query": "x"}})
    assert isinstance(f, OpenLibraryFetcher)


def test_build_fetcher_googlenews() -> None:
    f = build_fetcher({"source": "googlenews", "config": {"query": "x"}})
    assert isinstance(f, GoogleNewsFetcher)


def test_build_fetcher_guardian() -> None:
    f = build_fetcher({"source": "guardian", "config": {"query": "x"}})
    assert isinstance(f, GuardianFetcher)


def test_build_fetcher_raises_on_unknown_source() -> None:
    with pytest.raises(ValidationError, match="Input should be"):
        build_fetcher({"source": "unknown", "config": {"query": "x"}})
