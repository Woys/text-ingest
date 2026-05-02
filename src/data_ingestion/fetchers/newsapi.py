"""NewsAPI.org fetcher (free developer tier)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import NewsApiConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.logging_utils import get_logger
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date

logger = get_logger(__name__)


@register_fetcher("newsapi")
class NewsApiFetcher(BaseFetcher):
    """Fetches news articles from newsapi.org /v2/everything."""

    BASE_URL = "https://newsapi.org/v2/everything"
    config_model = NewsApiConfig

    def __init__(self, config: NewsApiConfig) -> None:
        super().__init__(config)
        self.config: NewsApiConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "newsapi"

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        """Parse newsapi's ISO-8601 datetime string to a datetime.date."""
        from datetime import datetime, timezone

        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).date()
        except ValueError:
            return None

    @staticmethod
    def _extract_topic(item: dict[str, Any]) -> str | None:
        source = item.get("source") or {}
        source_name = source.get("name")
        if isinstance(source_name, str) and source_name.strip():
            return source_name.strip()
        return None

    def _request_languages(self) -> list[str]:
        if self.config.languages:
            return self.config.languages
        return [self.config.language]

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        author_raw: str | None = item.get("author")
        authors = [author_raw.strip()] if author_raw and author_raw.strip() else []

        title = item.get("title")
        pub_date = self._parse_date(item.get("publishedAt"))
        abstract = item.get("description")
        url = item.get("url")
        full_text = item.get("content")

        return NormalizedRecord(
            source=self.source_name,
            external_id=url,
            title=title,
            authors=authors,
            published_date=pub_date,
            url=url,
            abstract=abstract,
            full_text=full_text,
            full_text_url=url,
            topic=self._extract_topic(item),
            record_type=RecordType.NEWS,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("language") or item.get("_requested_language")
        if isinstance(raw, str):
            return raw
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        for language in self._request_languages():
            pages_fetched = 0
            page = 1
            while not self._page_limit_reached(pages_fetched):
                params: dict[str, Any] = {
                    "q": self.config.query,
                    "language": language,
                    "pageSize": self.config.page_size,
                    "page": page,
                    "apiKey": self.config.api_key,
                }
                if self.config.start_date is not None:
                    params["from"] = self.config.start_date.isoformat()
                if self.config.end_date is not None:
                    params["to"] = self.config.end_date.isoformat()

                logger.info(
                    "NewsAPI: requesting language=%s page=%d page_size=%d "
                    "start_date=%s end_date=%s",
                    language,
                    page,
                    self.config.page_size,
                    self.config.start_date,
                    self.config.end_date,
                )
                try:
                    response = self.session.get(
                        self.BASE_URL,
                        params=params,
                        timeout=self.config.http.timeout_seconds,
                    )
                    response.raise_for_status()
                    payload: dict[str, Any] = response.json()
                except requests.RequestException as exc:
                    raise FetcherError(
                        f"NewsAPI request failed on page {page}: {exc}"
                    ) from exc
                except ValueError as exc:
                    raise FetcherError(
                        f"NewsAPI returned invalid JSON on page {page}"
                    ) from exc

                if payload.get("status") != "ok":
                    raise FetcherError(
                        f"NewsAPI error on page {page}: "
                        f"{payload.get('code')} — {payload.get('message')}"
                    )

                raw_articles: list[dict[str, Any]] = payload.get("articles", [])
                if not raw_articles:
                    logger.info(
                        "NewsAPI: no articles for language=%s on page %d — stopping",
                        language,
                        page,
                    )
                    break

                articles: list[dict[str, Any]] = []
                for article in raw_articles:
                    annotated = dict(article)
                    annotated["_requested_language"] = language
                    articles.append(annotated)

                logger.info(
                    "NewsAPI: received language=%s page=%d articles=%d "
                    "total_results=%s",
                    language,
                    page,
                    len(articles),
                    payload.get("totalResults"),
                )
                yield articles
                pages_fetched += 1

                if len(raw_articles) < self.config.page_size:
                    logger.info(
                        "NewsAPI: partial page (%d < %d) for language=%s "
                        "— stopping after page %d",
                        len(raw_articles),
                        self.config.page_size,
                        language,
                        page,
                    )
                    break
                page += 1
            else:
                logger.info(
                    "NewsAPI: stopped after max_pages language=%s pages_fetched=%d",
                    language,
                    pages_fetched,
                )
