"""Reddit JSON API fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import RedditConfig
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


@register_fetcher("reddit")
class RedditFetcher(BaseFetcher):
    """Fetches Reddit discussion posts from search/new feeds."""

    BASE_URL = "https://www.reddit.com/search.json"
    config_model = RedditConfig

    def __init__(self, config: RedditConfig) -> None:
        super().__init__(config)
        self.config: RedditConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "reddit"

    @staticmethod
    def _parse_date(raw: float | int | None) -> date | None:
        from datetime import datetime, timezone

        if raw is None:
            return None
        with contextlib.suppress(ValueError, OSError):
            dt = datetime.fromtimestamp(float(raw), tz=timezone.utc)
            return dt.date()
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        permalink = item.get("permalink") or ""
        url = f"https://www.reddit.com{permalink}" if permalink else item.get("url")
        author_raw = item.get("author")
        authors = [author_raw] if isinstance(author_raw, str) and author_raw else []

        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("id"),
            title=item.get("title"),
            authors=authors,
            published_date=self._parse_date(item.get("created_utc")),
            url=url,
            abstract=item.get("selftext"),
            full_text=item.get("selftext"),
            full_text_url=url,
            topic=item.get("subreddit"),
            record_type=RecordType.NEWS,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("lang")
        if isinstance(raw, str):
            return raw
        return "en"

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        after: str | None = None
        pages_fetched = 0
        page = 0
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "q": self.config.query,
                "sort": self.config.sort,
                "limit": self.config.page_size,
                "restrict_sr": bool(self.config.subreddit),
                "raw_json": 1,
            }
            if self.config.subreddit:
                params["q"] = f"subreddit:{self.config.subreddit} {self.config.query}"
            if after:
                params["after"] = after

            logger.info(
                "Reddit: requesting page=%d limit=%d subreddit=%s after=%s",
                page + 1,
                self.config.page_size,
                self.config.subreddit,
                after,
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
                    f"Reddit request failed on page {page + 1}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"Reddit returned invalid JSON on page {page + 1}"
                ) from exc

            children = payload.get("data", {}).get("children", [])
            if not children:
                logger.info(
                    "Reddit: no children page=%d pages_fetched=%d",
                    page + 1,
                    pages_fetched,
                )
                return

            items: list[dict[str, Any]] = [
                {**child.get("data", {}), "lang": "en"} for child in children
            ]
            logger.info(
                "Reddit: received page=%d items=%d next_after=%s",
                page + 1,
                len(items),
                payload.get("data", {}).get("after"),
            )
            yield items
            pages_fetched += 1

            after = payload.get("data", {}).get("after")
            if not after:
                logger.info(
                    "Reddit: no next page pages_fetched=%d",
                    pages_fetched,
                )
                return
            page += 1
        logger.info("Reddit: stopped after max_pages pages_fetched=%d", pages_fetched)
