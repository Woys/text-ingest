"""US Federal Register fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from data_ingestion.config import FederalRegisterConfig
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


@register_fetcher("federalregister")
class FederalRegisterFetcher(BaseFetcher):
    BASE_URL = "https://www.federalregister.gov/api/v1/articles.json"
    config_model = FederalRegisterConfig

    def __init__(self, config: FederalRegisterConfig) -> None:
        super().__init__(config)
        self.config: FederalRegisterConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "federalregister"

    @staticmethod
    def _extract_topic(item: dict[str, Any]) -> str | None:
        agencies = item.get("agencies") or []
        for agency in agencies:
            name = agency.get("raw_name") or agency.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()

        item_type = item.get("type")
        if isinstance(item_type, str) and item_type.strip():
            return item_type.strip()

        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        from datetime import date

        pub_date: date | None = None
        if raw := item.get("publication_date"):
            with contextlib.suppress(ValueError):
                pub_date = date.fromisoformat(raw)

        agencies = [
            a.get("raw_name") or a.get("name")
            for a in item.get("agencies", [])
            if a.get("raw_name") or a.get("name")
        ]

        # Grab text URL, fallback to PDF URL, fallback to HTML URL.
        ft_url = item.get("raw_text_url") or item.get("pdf_url") or item.get("html_url")

        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("document_number"),
            title=item.get("title"),
            authors=agencies,
            published_date=pub_date,
            url=item.get("html_url"),
            abstract=item.get("abstract"),
            full_text=None,
            full_text_url=ft_url,
            topic=self._extract_topic(item),
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        pages_fetched = 0
        page = 1
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "per_page": self.config.per_page,
                "page": page,
                "order": "newest",
            }

            if self.config.query:
                params["conditions[term]"] = self.config.query

            if self.config.start_date is not None:
                params["conditions[publication_date][gte]"] = (
                    self.config.start_date.isoformat()
                )
            if self.config.end_date is not None:
                params["conditions[publication_date][lte]"] = (
                    self.config.end_date.isoformat()
                )

            try:
                res = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.config.http.timeout_seconds,
                )
                res.raise_for_status()
                payload = res.json()
            except Exception as exc:
                raise FetcherError(f"FederalRegister fail: {exc}") from exc

            results = payload.get("results", [])
            if not results:
                return

            yield results
            pages_fetched += 1

            if page >= payload.get("total_pages", 0):
                return
            page += 1

    def extract_language(self, item: dict[str, Any]) -> str | None:
        del item
        return "en"
