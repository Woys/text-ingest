"""Shared pytest fixtures."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from data_ingestion.config import JsonlSinkConfig
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.sinks.jsonl import JsonlSink


class FakeResponse:
    """Minimal stand-in for a requests.Response."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


@pytest.fixture()
def fake_response_factory():
    return FakeResponse


@pytest.fixture()
def tmp_jsonl_sink(tmp_path):
    return JsonlSink(
        JsonlSinkConfig(output_file=str(tmp_path / "output.jsonl"), append=False)
    )


@pytest.fixture()
def sample_record() -> NormalizedRecord:
    return NormalizedRecord(
        source="openalex",
        external_id="https://openalex.org/W1",
        title="A study on cancer prevention",
        authors=["Alice Smith", "Bob Jones"],
        published_date=date(2024, 6, 15),
        url="https://doi.org/10.1000/test",
        abstract="A short abstract.",
        record_type=RecordType.ARTICLE,
        raw_payload={
            "id": "https://openalex.org/W1",
            "title": "A study on cancer prevention",
        },
    )
