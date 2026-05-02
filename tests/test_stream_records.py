from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from data_ingestion.models import NormalizedRecord
from data_ingestion.pipeline import (
    async_stream_records,
    async_stream_transformed_records,
    stream_records,
    stream_transformed_records,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _RawFetcher:
    @property
    def source_name(self) -> str:
        return "raw-source"

    def fetch_raw(self):
        yield {"id": "r1"}
        yield {"id": "r2"}

    def fetch_all(self):
        raise AssertionError("fetch_all should not be used in raw mode")

    async def async_fetch_raw(self) -> AsyncIterator[dict[str, str]]:
        for item in self.fetch_raw():
            await asyncio.sleep(0)
            yield item

    async def async_fetch_all(self) -> AsyncIterator[NormalizedRecord]:
        raise AssertionError("async_fetch_all should not be used in raw mode")


class _NormalizedFetcher:
    def __init__(self, source_name: str = "norm-source", external_id: str = "n1"):
        self._source_name = source_name
        self._external_id = external_id

    @property
    def source_name(self) -> str:
        return self._source_name

    def fetch_raw(self):
        raise AssertionError("fetch_raw should not be used in normalized mode")

    def fetch_all(self):
        yield NormalizedRecord(
            source=self.source_name,
            external_id=self._external_id,
            raw_payload={},
        )

    async def async_fetch_raw(self) -> AsyncIterator[dict[str, str]]:
        raise AssertionError("async_fetch_raw should not be used in normalized mode")

    async def async_fetch_all(self) -> AsyncIterator[NormalizedRecord]:
        for record in self.fetch_all():
            await asyncio.sleep(0)
            yield record


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


def test_async_stream_records_raw_mode(monkeypatch) -> None:
    async def collect_items():
        return [
            item
            async for item in async_stream_records(
                [{"source": "openalex", "config": {}}],
                raw=True,
            )
        ]

    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_RawFetcher()],
    )

    items = asyncio.run(collect_items())
    assert items == [
        ("raw-source", {"id": "r1"}),
        ("raw-source", {"id": "r2"}),
    ]


def test_async_stream_records_normalized_mode(monkeypatch) -> None:
    async def collect_items():
        return [
            item
            async for item in async_stream_records(
                [{"source": "openalex", "config": {}}],
                raw=False,
            )
        ]

    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_NormalizedFetcher()],
    )

    items = asyncio.run(collect_items())
    assert len(items) == 1
    assert items[0][0] == "norm-source"
    assert items[0][1].external_id == "n1"


def test_async_stream_records_rejects_transform_spec_in_raw_mode(monkeypatch) -> None:
    async def collect_items() -> None:
        async for _ in async_stream_records(
            [{"source": "openalex", "config": {}}],
            raw=True,
            transform_spec={"transforms": []},
        ):
            pass

    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_RawFetcher()],
    )

    with pytest.raises(ValueError, match="only supported when raw=False"):
        asyncio.run(collect_items())


def test_async_stream_transformed_records_applies_spec(monkeypatch) -> None:
    async def collect_items():
        return [
            item
            async for item in async_stream_transformed_records(
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
        ]

    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_NormalizedFetcher()],
    )

    items = asyncio.run(collect_items())
    assert len(items) == 1
    assert items[0][0] == "norm-source"
    assert items[0][1].topic == "library-controlled"


def test_async_stream_records_concurrent_sources(monkeypatch) -> None:
    async def collect_items():
        return [
            item
            async for item in async_stream_records(
                [
                    {"source": "openalex", "config": {}},
                    {"source": "crossref", "config": {}},
                ],
                raw=False,
                transform_spec={"transforms": [{"op": "dedupe"}]},
                concurrent_sources=True,
                max_source_concurrency=2,
            )
        ]

    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [
            _NormalizedFetcher("source-a", "shared-id"),
            _NormalizedFetcher("source-b", "shared-id"),
        ],
    )

    items = asyncio.run(collect_items())
    assert sorted(source for source, _ in items) == ["source-a", "source-b"]
    assert [record.external_id for _, record in items] == ["shared-id", "shared-id"]


def test_async_stream_records_rejects_invalid_queue_size() -> None:
    async def collect_items() -> None:
        async for _ in async_stream_records(
            [{"source": "openalex", "config": {}}],
            raw=False,
            max_async_queue_size=0,
        ):
            pass

    with pytest.raises(ValueError, match="max_async_queue_size"):
        asyncio.run(collect_items())


def test_async_stream_records_uses_bounded_queue(monkeypatch) -> None:
    original_queue = asyncio.Queue
    queue_sizes: list[int] = []

    def queue_factory(*args, **kwargs):
        queue_sizes.append(kwargs.get("maxsize", 0))
        return original_queue(*args, **kwargs)

    async def collect_items():
        return [
            item
            async for item in async_stream_records(
                [{"source": "openalex", "config": {}}],
                raw=False,
                concurrent_sources=True,
                max_async_queue_size=3,
            )
        ]

    monkeypatch.setattr("data_ingestion.pipeline.asyncio.Queue", queue_factory)
    monkeypatch.setattr(
        "data_ingestion.pipeline.build_fetchers",
        lambda specs: [_NormalizedFetcher()],
    )

    items = asyncio.run(collect_items())
    assert len(items) == 1
    assert queue_sizes == [3]
