"""Newline-delimited JSON sink for full-text documents."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion.exceptions import SinkError

if TYPE_CHECKING:
    from types import TracebackType
    from typing import IO

    from data_ingestion.config import FullTextSinkConfig
    from data_ingestion.models import FullTextDocument


class FullTextJsonlSink:
    """Writes FullTextDocument objects to a JSONL file."""

    def __init__(self, config: FullTextSinkConfig) -> None:
        self.config = config
        self._handle: IO[str] | None = None

    def __enter__(self) -> FullTextJsonlSink:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._handle is not None:
            return
        try:
            path = Path(self.config.output_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            mode = "a" if self.config.append else "w"
            self._handle = path.open(mode, encoding=self.config.encoding)
        except OSError as exc:
            raise SinkError(
                f"Cannot open full-text output file '{self.config.output_file}': {exc}"
            ) from exc

    def write(self, document: FullTextDocument) -> None:
        self._ensure_open()
        assert self._handle is not None
        try:
            self._handle.write(document.to_json_line() + "\n")
        except OSError as exc:
            raise SinkError(
                f"Cannot write to full-text output file "
                f"{self.config.output_file}: {exc}"
            ) from exc

    def close(self) -> None:
        if self._handle is not None and not self._handle.closed:
            self._handle.flush()
            self._handle.close()
