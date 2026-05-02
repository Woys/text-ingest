from __future__ import annotations

from datetime import date

import pytest
import requests

from data_ingestion.config import GitHubConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.fetchers.github import GitHubFetcher


class _Resp:
    def __init__(self, payload) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_normalize_maps_fields() -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1))
    rec = fetcher.normalize(
        {
            "id": 1,
            "full_name": "org/repo",
            "updated_at": "2026-03-15T12:00:00Z",
            "html_url": "https://github.com/org/repo",
            "description": "desc",
            "language": "Python",
            "owner": {"login": "org"},
        }
    )
    assert rec.external_id == "1"
    assert rec.title == "org/repo"
    assert rec.published_date == date(2026, 3, 15)
    assert rec.authors == ["org"]


def test_normalize_missing_date() -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1))
    rec = fetcher.normalize({"id": 1, "updated_at": None})
    assert rec.published_date is None


def test_normalize_invalid_date() -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1))
    rec = fetcher.normalize({"id": 1, "updated_at": "invalid"})
    assert rec.published_date is None


def test_extract_language() -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1))
    assert fetcher.extract_language({"human_language": "en"}) == "en"
    assert fetcher.extract_language({}) is None


def test_github_token_header() -> None:
    fetcher = GitHubFetcher(
        GitHubConfig(query="data", max_pages=1, github_token="secret")
    )
    assert fetcher.session.headers["Authorization"] == "Bearer secret"


def test_fetch_pages_success(monkeypatch) -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1, per_page=2))
    payload = {"items": [{"id": 1}, {"id": 2}]}
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 1
    assert [item["id"] for item in pages[0]] == [1, 2]


def test_fetch_pages_wraps_request_error(monkeypatch) -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1))

    def boom(*a, **k):
        del a, k
        raise requests.RequestException("boom")

    monkeypatch.setattr(fetcher.session, "get", boom)
    with pytest.raises(FetcherError, match="GitHub request failed"):
        list(fetcher.fetch_pages())


def test_fetch_pages_empty_items(monkeypatch) -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1))
    payload = {"items": []}
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _Resp(payload))
    pages = list(fetcher.fetch_pages())
    assert len(pages) == 0


class _RespInvalidJson:
    def raise_for_status(self) -> None:
        pass

    def json(self):
        raise ValueError("Invalid JSON")


def test_fetch_pages_invalid_json(monkeypatch) -> None:
    fetcher = GitHubFetcher(GitHubConfig(query="data", max_pages=1))
    monkeypatch.setattr(fetcher.session, "get", lambda *a, **k: _RespInvalidJson())
    with pytest.raises(FetcherError, match="invalid JSON"):
        list(fetcher.fetch_pages())
