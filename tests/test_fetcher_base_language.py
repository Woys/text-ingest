from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel

from data_ingestion.fetchers.base import BaseFetcher
from data_ingestion.models import NormalizedRecord


class _DummyConfig(BaseModel):
    languages: list[str] = []


class _DummyFetcher(BaseFetcher):
    config_model = _DummyConfig

    @property
    def source_name(self) -> str:
        return "dummy"

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("lang")
        return raw if isinstance(raw, str) else None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("id"),
            title=item.get("id"),
            raw_payload=item,
        )

    def fetch_pages(self):
        yield [
            {"id": "a", "lang": "en"},
            {"id": "b", "lang": "fr"},
            {"id": "c", "lang": "en-US"},
            {"id": "d"},
        ]


def test_fetch_all_filters_by_languages() -> None:
    fetcher = _DummyFetcher(_DummyConfig(languages=["en"]))
    records = list(fetcher.fetch_all())
    assert [rec.external_id for rec in records] == ["a", "c"]


def test_fetch_all_without_languages_keeps_all() -> None:
    fetcher = _DummyFetcher(_DummyConfig())
    records = list(fetcher.fetch_all())
    assert [rec.external_id for rec in records] == ["a", "b", "c", "d"]


def test_async_fetch_pages_exhausts_pages() -> None:
    async def collect_pages() -> list[list[dict[str, Any]]]:
        fetcher = _DummyFetcher(_DummyConfig())
        return [page async for page in fetcher.async_fetch_pages()]

    pages = asyncio.run(collect_pages())
    assert len(pages) == 1
    assert [item["id"] for item in pages[0]] == ["a", "b", "c", "d"]


def test_async_fetch_raw_filters_by_languages() -> None:
    async def collect_raw() -> list[dict[str, Any]]:
        fetcher = _DummyFetcher(_DummyConfig(languages=["en"]))
        return [item async for item in fetcher.async_fetch_raw()]

    items = asyncio.run(collect_raw())
    assert [item["id"] for item in items] == ["a", "c"]


def test_async_fetch_all_returns_normalized_records() -> None:
    async def collect_records() -> list[NormalizedRecord]:
        fetcher = _DummyFetcher(_DummyConfig(languages=["en"]))
        return [record async for record in fetcher.async_fetch_all()]

    records = asyncio.run(collect_records())
    assert [record.external_id for record in records] == ["a", "c"]
