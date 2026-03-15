import json

from data_ingestion.config import JsonlSinkConfig
from data_ingestion.models import NormalizedRecord
from data_ingestion.sinks.jsonl import JsonlSink


def test_sink_writes_single_record(tmp_jsonl_sink, sample_record, tmp_path) -> None:
    tmp_jsonl_sink.write(sample_record)
    tmp_jsonl_sink.close()

    lines = (tmp_path / "output.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["source"] == "openalex"
    assert parsed["external_id"] == "https://openalex.org/W1"
    assert parsed["title"] == "A study on cancer prevention"
    assert parsed["authors"] == ["Alice Smith", "Bob Jones"]
    assert parsed["published_date"] == "2024-06-15"
    assert parsed["record_type"] == "article"


def test_sink_appends_by_default(tmp_path) -> None:
    path = str(tmp_path / "out.jsonl")
    r1 = NormalizedRecord(source="s", external_id="1", raw_payload={})
    r2 = NormalizedRecord(source="s", external_id="2", raw_payload={})

    with JsonlSink(JsonlSinkConfig(output_file=path, append=False)) as s1:
        s1.write(r1)
    with JsonlSink(JsonlSinkConfig(output_file=path, append=True)) as s2:
        s2.write(r2)

    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    assert len(lines) == 2


def test_sink_creates_parent_dirs(tmp_path) -> None:
    path = str(tmp_path / "deep" / "nested" / "out.jsonl")
    sink = JsonlSink(JsonlSinkConfig(output_file=path, append=False))
    sink.write(NormalizedRecord(source="s", raw_payload={}))
    sink.close()
    assert (tmp_path / "deep" / "nested" / "out.jsonl").exists()


def test_context_manager_closes_handle(tmp_path) -> None:
    path = str(tmp_path / "ctx.jsonl")
    with JsonlSink(JsonlSinkConfig(output_file=path, append=False)) as sink:
        sink.write(NormalizedRecord(source="s", raw_payload={}))
    assert sink._handle is None or sink._handle.closed
