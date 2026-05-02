from __future__ import annotations

import csv
import json

import pytest

from data_ingestion.config import CsvSinkConfig
from data_ingestion.exceptions import SinkError
from data_ingestion.models import NormalizedRecord
from data_ingestion.sinks.csv import CsvSink


def _record(external_id: str) -> NormalizedRecord:
    return NormalizedRecord(
        source="openalex",
        external_id=external_id,
        title="Title",
        authors=["Alice", "Bob"],
        raw_payload={"x": 1},
    )


def test_csv_sink_write_single_record(tmp_path) -> None:
    path = tmp_path / "out.csv"
    sink = CsvSink(CsvSinkConfig(output_file=str(path), append=False))
    sink.write(_record("1"))
    sink.close()

    rows = list(csv.DictReader(path.open("r", encoding="utf-8")))
    assert len(rows) == 1
    assert rows[0]["external_id"] == "1"
    assert json.loads(rows[0]["authors"]) == ["Alice", "Bob"]
    assert json.loads(rows[0]["raw_payload"]) == {"x": 1}


def test_csv_sink_write_many_and_append(tmp_path) -> None:
    path = tmp_path / "out.csv"

    s1 = CsvSink(CsvSinkConfig(output_file=str(path), append=False))
    s1.write_many([_record("1"), _record("2")])
    s1.close()

    s2 = CsvSink(CsvSinkConfig(output_file=str(path), append=True))
    s2.write(_record("3"))
    s2.close()

    rows = list(csv.DictReader(path.open("r", encoding="utf-8")))
    assert [row["external_id"] for row in rows] == ["1", "2", "3"]


def test_csv_sink_without_raw_payload(tmp_path) -> None:
    path = tmp_path / "no_raw.csv"
    sink = CsvSink(
        CsvSinkConfig(output_file=str(path), append=False),
        include_raw_payload=False,
    )
    sink.write(_record("1"))
    sink.close()

    row = next(csv.DictReader(path.open("r", encoding="utf-8")))
    assert "raw_payload" not in row


def test_csv_sink_open_error(monkeypatch, tmp_path) -> None:
    path = tmp_path / "bad" / "out.csv"

    def boom(*args, **kwargs):
        raise OSError("nope")

    monkeypatch.setattr("pathlib.Path.open", boom)

    sink = CsvSink(CsvSinkConfig(output_file=str(path), append=False))
    with pytest.raises(SinkError, match="Cannot open output file"):
        sink.write(_record("1"))


def test_csv_sink_writerow_error(monkeypatch, tmp_path) -> None:
    path = tmp_path / "err.csv"
    sink = CsvSink(CsvSinkConfig(output_file=str(path), append=False))

    class _Writer:
        def writerow(self, row):
            raise csv.Error("bad-row")

    sink._ensure_open(["source"])
    sink._writer = _Writer()  # type: ignore[assignment]

    with pytest.raises(SinkError, match="Failed to write CSV row"):
        sink.write(_record("1"))


def test_csv_sink_writerows_error(monkeypatch, tmp_path) -> None:
    path = tmp_path / "errs.csv"
    sink = CsvSink(CsvSinkConfig(output_file=str(path), append=False))

    class _Writer:
        def writerow(self, row):
            raise csv.Error("bad-rows")

    sink._ensure_open(["source"])
    sink._writer = _Writer()  # type: ignore[assignment]

    with pytest.raises(SinkError, match="Failed to write CSV rows"):
        sink.write_many([_record("1")])


def test_csv_sink_write_many_streams_rows(monkeypatch, tmp_path) -> None:
    path = tmp_path / "stream.csv"
    sink = CsvSink(CsvSinkConfig(output_file=str(path), append=False))
    written_rows: list[dict[str, object]] = []

    class _Writer:
        def writerow(self, row):
            written_rows.append(row)

        def writerows(self, rows):
            raise AssertionError("writerows should not be used")

    def ensure_open(fieldnames):
        del fieldnames
        sink._writer = _Writer()  # type: ignore[assignment]

    monkeypatch.setattr(sink, "_ensure_open", ensure_open)
    sink.write_many([_record("1"), _record("2")])

    assert [row["external_id"] for row in written_rows] == ["1", "2"]
