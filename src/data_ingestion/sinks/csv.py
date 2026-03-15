from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from data_ingestion.exceptions import SinkError

from .base import BaseSink

if TYPE_CHECKING:
    from typing import IO

    from data_ingestion.config import CsvSinkConfig
    from data_ingestion.models import NormalizedRecord


class CsvSink(BaseSink):
    def __init__(
        self, config: CsvSinkConfig, *, include_raw_payload: bool = True
    ) -> None:
        self.config = config
        self.include_raw_payload = include_raw_payload
        self._handle: IO[str] | None = None
        self._writer: csv.DictWriter[Any] | None = None

    def _ensure_open(self, fieldnames: list[str]) -> None:
        if self._handle is not None:
            return
        try:
            path = Path(self.config.output_file)
            path.parent.mkdir(parents=True, exist_ok=True)

            write_header = not path.exists() or not self.config.append
            mode = "a" if self.config.append else "w"

            self._handle = path.open(mode, encoding="utf-8", newline="")
            self._writer = csv.DictWriter(self._handle, fieldnames=fieldnames)

            if write_header:
                self._writer.writeheader()
        except OSError as exc:
            raise SinkError(
                f"Cannot open output file '{self.config.output_file}': {exc}"
            ) from exc

    def _record_to_row(self, record: NormalizedRecord) -> dict[str, Any]:
        row = record.to_output_dict(include_raw_payload=self.include_raw_payload)
        row["authors"] = json.dumps(row["authors"])
        if "raw_payload" in row:
            row["raw_payload"] = json.dumps(row["raw_payload"])
        return row

    def write(self, record: NormalizedRecord) -> None:
        row = self._record_to_row(record)
        self._ensure_open(list(row.keys()))
        assert self._writer is not None
        try:
            self._writer.writerow(row)
        except csv.Error as exc:
            raise SinkError(f"Failed to write CSV row: {exc}") from exc

    def write_many(self, records: list[NormalizedRecord]) -> None:
        if not records:
            return
        rows = [self._record_to_row(record) for record in records]
        self._ensure_open(list(rows[0].keys()))
        assert self._writer is not None
        try:
            self._writer.writerows(rows)
        except csv.Error as exc:
            raise SinkError(f"Failed to write CSV rows: {exc}") from exc

    def close(self) -> None:
        if self._handle is not None and not self._handle.closed:
            self._handle.flush()
            self._handle.close()
