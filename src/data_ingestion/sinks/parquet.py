"""Columnar Parquet sink."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from data_ingestion.exceptions import SinkError

from .base import BaseSink

if TYPE_CHECKING:
    from data_ingestion.config import ParquetSinkConfig
    from data_ingestion.models import NormalizedRecord


class ParquetSink(BaseSink):
    """Writes NormalizedRecords to a strictly typed Parquet file in batches."""

    def __init__(self, config: ParquetSinkConfig) -> None:
        self.config = config
        self._buffer: list[dict[str, Any]] = []
        self._writer: pq.ParquetWriter | None = None

        # Ensure output directory exists
        path = Path(self.config.output_file)
        path.parent.mkdir(parents=True, exist_ok=True)

    def _flush(self) -> None:
        """Convert the buffer to a PyArrow Table and write to disk."""
        if not self._buffer:
            return

        try:
            # Convert python dicts to a PyArrow Table
            table = pa.Table.from_pylist(self._buffer)

            # Initialize the writer on the first flush so we can extract the schema
            if self._writer is None:
                self._writer = pq.ParquetWriter(
                    self.config.output_file,
                    table.schema,
                    compression=self.config.compression,
                )

            self._writer.write_table(table)
            self._buffer.clear()

        except Exception as exc:
            raise SinkError(f"Failed to write Parquet batch: {exc}") from exc

    def write(self, record: NormalizedRecord) -> None:
        # 1. Dump the record to a standard python dictionary
        row = record.model_dump(mode="python")

        # 2. Extract enum values (e.g., RecordType.ARTICLE -> 'article')
        row["record_type"] = row["record_type"].value

        # 3. CRITICAL: Stringify the raw payload to avoid Parquet schema conflicts
        row["raw_payload"] = json.dumps(row["raw_payload"])

        self._buffer.append(row)

        # 4. Flush if we hit the batch size limits
        if len(self._buffer) >= self.config.batch_size:
            self._flush()

    def close(self) -> None:
        """Flush any remaining records and close the Parquet file."""
        self._flush()
        if self._writer is not None:
            self._writer.close()
