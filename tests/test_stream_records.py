from __future__ import annotations

import pytest

from data_ingestion.models import NormalizedRecord
from data_ingestion.pipeline import stream_records, stream_transformed_records


class _RawFetcher:
    @property
    def source_name(self) -> str:
        return "raw-source"

    def fetch_raw(self):
        yield {"id": "r1"}
        yield {"id": "r2"}

    def fetch_all(self):
        raise AssertionError("fetch_all should not be used in raw mode")


class _NormalizedFetcher:
    @property
    def source_name(self) -> str:
        return "norm-source"

    def fetch_raw(self):
        raise AssertionError("fetch_raw should not be used in normalized mode")

    def fetch_all(self):
        yield NormalizedRecord(source="norm-source", external_id="n1", raw_payload={})


def test_stream_records_raw_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_RawFetcher()],
    )

    items = list(stream_records([{"source": "openalex", "config": {}}], raw=True))
    assert items == [
        ("raw-source", {"id": "r1"}),
        ("raw-source", {"id": "r2"}),
    ]


def test_stream_records_normalized_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_NormalizedFetcher()],
    )

    items = list(stream_records([{"source": "openalex", "config": {}}], raw=False))
    assert len(items) == 1
    assert items[0][0] == "norm-source"
    assert items[0][1].external_id == "n1"


def test_stream_records_rejects_transform_spec_in_raw_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_RawFetcher()],
    )

    with pytest.raises(ValueError, match="only supported when raw=False"):
        list(
            stream_records(
                [{"source": "openalex", "config": {}}],
                raw=True,
                transform_spec={"transforms": []},
            )
        )


def test_stream_transformed_records_applies_spec(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_NormalizedFetcher()],
    )

    items = list(
        stream_transformed_records(
            [{"source": "openalex", "config": {}}],
            transform_spec={
                "transforms": [
                    {
                        "op": "set_field",
                        "field": "topic",
                        "value": "library-controlled",
                    }
                ]
            },
        )
    )

    assert len(items) == 1
    assert items[0][0] == "norm-source"
    assert items[0][1].topic == "library-controlled"
