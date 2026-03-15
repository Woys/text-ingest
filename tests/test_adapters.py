import json

from data_ingestion.adapters.airflow import airflow_ingestion_task


def test_airflow_adapter_streams_and_uses_handler(monkeypatch) -> None:
    streamed = [
        ("openalex", {"id": "1"}),
        ("newsapi", {"id": "2"}),
        ("openalex", {"id": "3"}),
    ]

    def fake_stream(fetcher_specs, *, raw, start_date, end_date):
        assert raw is True
        assert start_date == "2026-01-01"
        assert end_date == "2026-01-31"
        yield from streamed

    monkeypatch.setattr("data_ingestion.adapters.airflow.stream_records", fake_stream)

    seen: list[tuple[str, dict[str, str]]] = []

    def handler(source, record):
        seen.append((source, record))

    result = airflow_ingestion_task(
        fetcher_specs=[{"source": "openalex", "config": {"query": "test"}}],
        raw=True,
        start_date="2026-01-01",
        end_date="2026-01-31",
        record_handler=handler,
    )

    assert result["total_records"] == 3
    assert result["by_source"] == {"openalex": 2, "newsapi": 1}
    assert result["raw_mode"] is True
    assert seen == streamed
    json.dumps(result)
