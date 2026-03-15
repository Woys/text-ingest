from data_ingestion.adapters.spark import (
    load_jsonl_to_dataframe,
    load_parquet_to_dataframe,
    records_to_dataframe,
    stream_records_for_spark,
)


class _FakeReader:
    def __init__(self) -> None:
        self.last_json_path: str | None = None
        self.last_parquet_path: str | None = None

    def json(self, path: str) -> dict[str, str]:
        self.last_json_path = path
        return {"loaded_json_path": path}

    def parquet(self, path: str) -> dict[str, str]:
        self.last_parquet_path = path
        return {"loaded_parquet_path": path}


class _FakeSpark:
    def __init__(self) -> None:
        self.read = _FakeReader()
        self.rows: object | None = None

    def createDataFrame(self, rows):  # noqa: N802
        self.rows = rows
        return {"dataframe_rows": rows}


def test_load_jsonl_to_dataframe() -> None:
    spark = _FakeSpark()
    result = load_jsonl_to_dataframe(spark, "/tmp/data.jsonl")
    assert result == {"loaded_json_path": "/tmp/data.jsonl"}
    assert spark.read.last_json_path == "/tmp/data.jsonl"


def test_load_parquet_to_dataframe() -> None:
    spark = _FakeSpark()
    result = load_parquet_to_dataframe(spark, "/tmp/data.parquet")
    assert result == {"loaded_parquet_path": "/tmp/data.parquet"}
    assert spark.read.last_parquet_path == "/tmp/data.parquet"


def test_records_to_dataframe() -> None:
    spark = _FakeSpark()
    rows = [{"source": "openalex", "id": "1"}]
    result = records_to_dataframe(spark, rows)
    assert result == {"dataframe_rows": rows}
    assert spark.rows == rows


def test_stream_records_for_spark(monkeypatch) -> None:
    streamed = [
        ("openalex", {"id": "1"}),
        ("crossref", {"id": "2"}),
    ]

    def fake_stream(fetcher_specs, *, raw, start_date, end_date):
        assert raw is True
        assert start_date == "2026-01-01"
        assert end_date == "2026-01-31"
        yield from streamed

    monkeypatch.setattr("data_ingestion.adapters.spark.stream_records", fake_stream)

    result = list(
        stream_records_for_spark(
            [{"source": "openalex", "config": {"query": "x"}}],
            raw=True,
            start_date="2026-01-01",
            end_date="2026-01-31",
        )
    )
    assert result == streamed
