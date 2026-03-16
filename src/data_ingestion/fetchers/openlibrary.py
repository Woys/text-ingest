"""Open Library search fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import OpenLibraryConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date


@register_fetcher("openlibrary")
class OpenLibraryFetcher(BaseFetcher):
    """Fetches book metadata from Open Library search API."""

    BASE_URL = "https://openlibrary.org/search.json"
    config_model = OpenLibraryConfig

    def __init__(self, config: OpenLibraryConfig) -> None:
        super().__init__(config)
        self.config: OpenLibraryConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "openlibrary"

    @staticmethod
    def _parse_date(raw: int | None) -> date | None:
        from datetime import date

        if raw is None:
            return None
        with contextlib.suppress(ValueError):
            return date(raw, 1, 1)
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        authors = [
            name for name in (item.get("author_name") or []) if isinstance(name, str)
        ]
        key = item.get("key")
        url = f"https://openlibrary.org{key}" if isinstance(key, str) else None

        return NormalizedRecord(
            source=self.source_name,
            external_id=key,
            title=item.get("title"),
            authors=authors,
            published_date=self._parse_date(item.get("first_publish_year")),
            url=url,
            abstract=None,
            full_text=None,
            full_text_url=url,
            topic=(item.get("subject") or [None])[0],
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        languages = item.get("language") or []
        if isinstance(languages, list):
            for language in languages:
                if isinstance(language, str) and language.strip():
                    return language.strip()
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        for page in range(1, self.config.max_pages + 1):
            params: dict[str, Any] = {
                "q": self.config.query,
                "limit": self.config.page_size,
                "page": page,
            }

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
                    f"OpenLibrary request failed on page {page}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"OpenLibrary returned invalid JSON on page {page}"
                ) from exc

            items: list[dict[str, Any]] = payload.get("docs", [])
            if not items:
                return

            yield items

            if len(items) < self.config.page_size:
                return
