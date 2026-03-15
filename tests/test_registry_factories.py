import pytest
from pydantic import ValidationError

from data_ingestion.factories import build_fetcher, build_fetchers
from data_ingestion.fetchers.crossref import CrossRefFetcher
from data_ingestion.fetchers.newsapi import NewsApiFetcher
from data_ingestion.fetchers.openalex import OpenAlexFetcher
from data_ingestion.registry import list_fetchers


def test_registry_contains_all_builtin_fetchers() -> None:
    fetchers = list_fetchers()
    assert "openalex" in fetchers
    assert "crossref" in fetchers
    assert "newsapi" in fetchers


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
        ]
    )
    assert len(fetchers) == 3


def test_build_fetcher_raises_on_unknown_source() -> None:
    with pytest.raises(ValidationError, match="Input should be"):
        build_fetcher({"source": "unknown", "config": {"query": "x"}})
