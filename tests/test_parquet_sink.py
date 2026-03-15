from __future__ import annotations

from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from data_ingestion.config import ParquetSinkConfig
from data_ingestion.exceptions import SinkError
from data_ingestion.models import NormalizedRecord
from data_ingestion.sinks.parquet import ParquetSink


def _record(external_id: str) -> NormalizedRecord:
    return NormalizedRecord(
        source="openalex",
        external_id=external_id,
        title="A",
        raw_payload={"k": external_id},
    )


def test_parquet_sink_write_and_close(tmp_path) -> None:
    path = tmp_path / "out.parquet"
    sink = ParquetSink(ParquetSinkConfig(output_file=str(path), batch_size=2))

    sink.write(_record("1"))
    sink.write(_record("2"))
    sink.close()

    table = pq.read_table(path)
    assert table.num_rows == 2


def test_parquet_sink_flushes_remaining_buffer_on_close(tmp_path) -> None:
    path = tmp_path / "buffered.parquet"
    sink = ParquetSink(ParquetSinkConfig(output_file=str(path), batch_size=100))
    sink.write(_record("1"))
    sink.close()

    table = pq.read_table(path)
    assert table.num_rows == 1


def test_parquet_sink_wraps_write_errors(monkeypatch, tmp_path) -> None:
    path = tmp_path / "bad.parquet"
    sink = ParquetSink(ParquetSinkConfig(output_file=str(path), batch_size=1))

    class _FakeTable:
        @staticmethod
        def from_pylist(rows):
            raise RuntimeError("explode")

    fake_pa = SimpleNamespace(Table=_FakeTable)
    monkeypatch.setattr("data_ingestion.sinks.parquet.pa", fake_pa)

    with pytest.raises(SinkError, match="Failed to write Parquet batch"):
        sink.write(_record("1"))
