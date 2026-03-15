from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType

    from data_ingestion.models import NormalizedRecord


class BaseSink(ABC):
    def __enter__(self) -> BaseSink:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    @abstractmethod
    def write(self, record: NormalizedRecord) -> None:
        """Persist a single normalized record."""

    def write_many(self, records: list[NormalizedRecord]) -> None:
        """Persist multiple records (default: iterate over write)."""
        for record in records:
            self.write(record)

    @abstractmethod
    def close(self) -> None:
        """Flush and release any underlying resources."""
