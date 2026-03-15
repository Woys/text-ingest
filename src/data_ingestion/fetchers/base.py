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

    def fetch_raw(self) -> Iterator[dict[str, Any]]:
        """Yield raw records from all pages without normalization."""
        for items in self.fetch_pages():
            yield from items

    def fetch_all(self) -> Iterator[NormalizedRecord]:
        """Yield normalized records (legacy convenience path)."""
        for item in self.fetch_raw():
            yield self.normalize(item)
