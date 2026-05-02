"""The Guardian Content API fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import GuardianConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date


@register_fetcher("guardian")
class GuardianFetcher(BaseFetcher):
    """Fetches news metadata from The Guardian Content API."""

    BASE_URL = "https://content.guardianapis.com/search"
    config_model = GuardianConfig

    def __init__(self, config: GuardianConfig) -> None:
        super().__init__(config)
        self.config: GuardianConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "guardian"

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        from datetime import datetime, timezone

        if not raw:
            return None
        with contextlib.suppress(ValueError):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).date()
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("id"),
            title=item.get("webTitle"),
            authors=[],
            published_date=self._parse_date(item.get("webPublicationDate")),
            url=item.get("webUrl"),
            abstract=None,
            full_text=None,
            full_text_url=item.get("webUrl"),
            topic=item.get("sectionName"),
            record_type=RecordType.NEWS,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("lang")
        if isinstance(raw, str):
            return raw
        return "en"

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        pages_fetched = 0
        page = 1
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "q": self.config.query,
                "api-key": self.config.api_key,
                "page-size": self.config.page_size,
                "page": page,
                "order-by": "newest",
            }
            if self.config.start_date is not None:
                params["from-date"] = self.config.start_date.isoformat()
            if self.config.end_date is not None:
                params["to-date"] = self.config.end_date.isoformat()

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
                    f"Guardian request failed on page {page}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"Guardian returned invalid JSON on page {page}"
                ) from exc

            content = payload.get("response", {})
            status = content.get("status")
            if status != "ok":
                raise FetcherError(
                    f"Guardian error on page {page}: {content.get('message') or status}"
                )

            results: list[dict[str, Any]] = content.get("results", [])
            if not results:
                return

            enriched = [{**item, "lang": "en"} for item in results]
            yield enriched
            pages_fetched += 1

            if page >= content.get("pages", 0):
                return
            page += 1
