"""Stack Exchange questions fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import StackExchangeConfig
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


@register_fetcher("stackexchange")
class StackExchangeFetcher(BaseFetcher):
    """Fetches questions from Stack Exchange network APIs."""

    BASE_URL = "https://api.stackexchange.com/2.3/questions"
    config_model = StackExchangeConfig

    def __init__(self, config: StackExchangeConfig) -> None:
        super().__init__(config)
        self.config: StackExchangeConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "stackexchange"

    @staticmethod
    def _parse_date(raw: int | float | None) -> date | None:
        from datetime import datetime, timezone

        if raw is None:
            return None
        with contextlib.suppress(ValueError, OSError):
            dt = datetime.fromtimestamp(float(raw), tz=timezone.utc)
            return dt.date()
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        owner_name = item.get("owner", {}).get("display_name")
        authors = [owner_name] if owner_name else []

        return NormalizedRecord(
            source=self.source_name,
            external_id=str(item.get("question_id"))
            if item.get("question_id") is not None
            else None,
            title=item.get("title"),
            authors=authors,
            published_date=self._parse_date(item.get("creation_date")),
            url=item.get("link"),
            abstract=None,
            full_text=None,
            full_text_url=item.get("link"),
            topic=(item.get("tags") or [None])[0],
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        del item
        return "en"

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        from datetime import datetime, time, timedelta, timezone

        pages_fetched = 0
        page = 1
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "site": self.config.site,
                "order": "desc",
                "sort": self.config.sort,
                "pagesize": self.config.page_size,
                "page": page,
                "filter": "default",
            }
            if self.config.query:
                params["q"] = self.config.query

            if self.config.start_date is not None:
                start_dt = datetime.combine(
                    self.config.start_date,
                    time.min,
                    tzinfo=timezone.utc,
                )
                params["fromdate"] = int(start_dt.timestamp())
            if self.config.end_date is not None:
                end_dt = datetime.combine(
                    self.config.end_date + timedelta(days=1),
                    time.min,
                    tzinfo=timezone.utc,
                )
                params["todate"] = int(end_dt.timestamp()) - 1

            logger.info(
                "StackExchange: requesting site=%s page=%d pagesize=%d "
                "fromdate=%s todate=%s",
                self.config.site,
                page,
                self.config.page_size,
                params.get("fromdate"),
                params.get("todate"),
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
                    f"StackExchange request failed on page {page}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"StackExchange returned invalid JSON on page {page}"
                ) from exc

            items: list[dict[str, Any]] = payload.get("items", [])
            if not items:
                logger.info(
                    "StackExchange: no items site=%s page=%d pages_fetched=%d",
                    self.config.site,
                    page,
                    pages_fetched,
                )
                return

            logger.info(
                "StackExchange: received site=%s page=%d items=%d has_more=%s "
                "quota_remaining=%s",
                self.config.site,
                page,
                len(items),
                payload.get("has_more"),
                payload.get("quota_remaining"),
            )
            yield items
            pages_fetched += 1

            if not payload.get("has_more", False):
                logger.info(
                    "StackExchange: no more pages site=%s pages_fetched=%d",
                    self.config.site,
                    pages_fetched,
                )
                return
            page += 1
        logger.info(
            "StackExchange: stopped after max_pages site=%s pages_fetched=%d",
            self.config.site,
            pages_fetched,
        )
