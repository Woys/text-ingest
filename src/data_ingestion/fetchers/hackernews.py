"""Hacker News fetcher (via Algolia search API)."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import HackerNewsConfig
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


@register_fetcher("hackernews")
class HackerNewsFetcher(BaseFetcher):
    """Fetches tech news and discussions from the Hacker News Algolia API."""

    BASE_URL = "https://hn.algolia.com/api/v1/search"
    config_model = HackerNewsConfig

    def __init__(self, config: HackerNewsConfig) -> None:
        super().__init__(config)
        self.config: HackerNewsConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "hackernews"

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        from datetime import datetime, timezone

        if not raw:
            return None
        with contextlib.suppress(ValueError):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).date()
        return None

    @staticmethod
    def _extract_topic(item: dict[str, Any]) -> str | None:
        tags = item.get("_tags") or []
        ignore_prefixes = ("author_", "story_", "comment_", "poll_")
        for tag in tags:
            if not isinstance(tag, str):
                continue
            if tag.startswith(ignore_prefixes):
                continue
            cleaned = tag.replace("_", " ").strip()
            if cleaned:
                return cleaned
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        author_raw: str | None = item.get("author")
        authors = [author_raw.strip()] if author_raw and author_raw.strip() else []

        title = item.get("title") or item.get("story_title")
        text_content = item.get("story_text") or item.get("comment_text")

        url = item.get("url")
        if not url and item.get("objectID"):
            url = f"https://news.ycombinator.com/item?id={item.get('objectID')}"

        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("objectID"),
            title=title,
            authors=authors,
            published_date=self._parse_date(item.get("created_at")),
            url=url,
            abstract=None,
            full_text=text_content,
            full_text_url=url,
            topic=self._extract_topic(item),
            record_type=RecordType.NEWS,
            raw_payload=item,
        )

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        from datetime import datetime, time, timedelta, timezone

        endpoint = (
            "https://hn.algolia.com/api/v1/search_by_date"
            if self.config.use_date_sort
            else self.BASE_URL
        )

        numeric_filters: list[str] = []

        if self.config.start_date is not None:
            start_dt = datetime.combine(
                self.config.start_date,
                time.min,
                tzinfo=timezone.utc,
            )
            numeric_filters.append(f"created_at_i>={int(start_dt.timestamp())}")

        if self.config.end_date is not None:
            end_exclusive = datetime.combine(
                self.config.end_date + timedelta(days=1),
                time.min,
                tzinfo=timezone.utc,
            )
            numeric_filters.append(f"created_at_i<{int(end_exclusive.timestamp())}")

        tags = []
        if self.config.hn_item_type == "story":
            tags.append("story")
        elif self.config.hn_item_type == "comment":
            tags.append("comment")

        pages_fetched = 0
        page = 0
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "hitsPerPage": self.config.hits_per_page,
                "page": page,
            }

            if self.config.query:
                params["query"] = self.config.query

            if tags:
                params["tags"] = ",".join(tags)

            if numeric_filters:
                params["numericFilters"] = ",".join(numeric_filters)

            try:
                response = self.session.get(
                    endpoint,
                    params=params,
                    timeout=self.config.http.timeout_seconds,
                )
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
            except requests.RequestException as exc:
                raise FetcherError(
                    f"HackerNews request failed on page {page}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"HackerNews returned invalid JSON on page {page}"
                ) from exc

            hits: list[dict[str, Any]] = payload.get("hits", [])
            if not hits:
                logger.info("HackerNews: no hits on page %d — stopping", page)
                return

            yield hits
            pages_fetched += 1

            nb_pages = payload.get("nbPages", 0)
            if page >= nb_pages - 1:
                logger.info("HackerNews: reached last available page %d", page)
                return
            page += 1

    def extract_language(self, item: dict[str, Any]) -> str | None:
        del item
        return "en"
