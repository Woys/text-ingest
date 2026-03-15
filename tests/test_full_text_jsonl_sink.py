from __future__ import annotations

import json

import pytest

from data_ingestion.config import FullTextSinkConfig
from data_ingestion.exceptions import SinkError
from data_ingestion.models import FullTextDocument
from data_ingestion.sinks.full_text_jsonl import FullTextJsonlSink


def _doc() -> FullTextDocument:
    return FullTextDocument(
        source="openalex",
        external_id="1",
        title="T",
        url="https://example.com",
        full_text_url="https://example.com/full",
        full_text="body",
    )


def test_full_text_sink_write_and_close(tmp_path) -> None:
    path = tmp_path / "full_text.jsonl"
    sink = FullTextJsonlSink(FullTextSinkConfig(output_file=str(path), append=False))
    sink.write(_doc())
    sink.close()

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["source"] == "openalex"


def test_full_text_sink_context_manager(tmp_path) -> None:
    path = tmp_path / "ctx.jsonl"
    config = FullTextSinkConfig(output_file=str(path), append=False)

    with FullTextJsonlSink(config) as sink:
        sink.write(_doc())

    assert sink._handle is None or sink._handle.closed


def test_full_text_sink_append_mode(tmp_path) -> None:
    path = tmp_path / "append.jsonl"
    with FullTextJsonlSink(
        FullTextSinkConfig(output_file=str(path), append=False)
    ) as sink:
        sink.write(_doc())

    with FullTextJsonlSink(
        FullTextSinkConfig(output_file=str(path), append=True)
    ) as sink:
        sink.write(_doc())

    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_full_text_sink_open_error(monkeypatch, tmp_path) -> None:
    path = tmp_path / "bad" / "out.jsonl"

    def boom(*args, **kwargs):
        raise OSError("nope")

    monkeypatch.setattr("pathlib.Path.open", boom)

    sink = FullTextJsonlSink(FullTextSinkConfig(output_file=str(path), append=False))
    with pytest.raises(SinkError, match="Cannot open full-text output file"):
        sink.write(_doc())
