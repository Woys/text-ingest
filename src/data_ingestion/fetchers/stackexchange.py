"""Stack Exchange questions fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import StackExchangeConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date


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
        for page in range(1, self.config.max_pages + 1):
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
                return

            yield items

            if not payload.get("has_more", False):
                return
