"""Abstract base class for all fetchers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic import BaseModel

    from data_ingestion.models import NormalizedRecord


class BaseFetcher(ABC):
    """Contract every fetcher must fulfil."""

    config_model: ClassVar[type[BaseModel]]

    def __init__(self, config: BaseModel) -> None:
        self.config = config

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Short identifier for the data source."""

    @abstractmethod
    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        """Map one raw API response item to the shared NormalizedRecord schema."""

    @abstractmethod
    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        """Yield raw API page payload items with minimal processing."""

    def extract_language(self, item: dict[str, Any]) -> str | None:
        """Return language code for *item* when available."""
        return None

    @staticmethod
    def _normalize_language_code(raw: str | None) -> str | None:
        if raw is None:
            return None
        cleaned = raw.strip().lower().replace("_", "-")
        return cleaned or None

    def _matches_language_filter(self, item: dict[str, Any]) -> bool:
        configured = getattr(self.config, "languages", [])
        if not configured:
            return True

        item_language = self._normalize_language_code(self.extract_language(item))
        if item_language is None:
            return False

        if item_language in configured:
            return True

        primary = item_language.split("-", 1)[0]
        return primary in configured

    def _page_limit_reached(self, pages_fetched: int) -> bool:
        max_pages = getattr(self.config, "max_pages", None)
        return max_pages is not None and max_pages > 0 and pages_fetched >= max_pages

    def fetch_raw(self) -> Iterator[dict[str, Any]]:
        """Yield raw records from all pages without normalization."""
        for items in self.fetch_pages():
            for item in items:
                if self._matches_language_filter(item):
                    yield item

    def fetch_all(self) -> Iterator[NormalizedRecord]:
        """Yield normalized records (legacy convenience path)."""
        for item in self.fetch_raw():
            yield self.normalize(item)
