from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_ingestion.exceptions import SinkError

from .base import BaseSink

if TYPE_CHECKING:
    from typing import IO

    from data_ingestion.config import JsonlSinkConfig
    from data_ingestion.models import NormalizedRecord


class JsonlSink(BaseSink):
    def __init__(
        self, config: JsonlSinkConfig, *, include_raw_payload: bool = True
    ) -> None:
        self.config = config
        self.include_raw_payload = include_raw_payload
        self._handle: IO[str] | None = None

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
                f"Cannot open output file '{self.config.output_file}': {exc}"
            ) from exc

    def write(self, record: NormalizedRecord) -> None:
        self._ensure_open()
        assert self._handle is not None
        try:
            self._handle.write(
                record.to_json_line(include_raw_payload=self.include_raw_payload) + "\n"
            )
        except OSError as exc:
            raise SinkError(
                f"Cannot write to output file '{self.config.output_file}': {exc}"
            ) from exc

    def write_many(self, records: list[NormalizedRecord]) -> None:
        if not records:
            return
        self._ensure_open()
        assert self._handle is not None
        try:
            payload = "".join(
                record.to_json_line(include_raw_payload=self.include_raw_payload) + "\n"
                for record in records
            )
            self._handle.write(payload)
        except OSError as exc:
            raise SinkError(
                f"Cannot write to output file '{self.config.output_file}': {exc}"
            ) from exc

    def close(self) -> None:
        if self._handle is not None and not self._handle.closed:
            self._handle.flush()
            self._handle.close()
