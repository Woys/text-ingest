import json
import sys

import pytest

from data_ingestion.cli import main


class _DummySummary:
    def model_dump_json(self, indent: int = 2) -> str:
        return json.dumps(
            {
                "total_records": 3,
                "by_source": {"openalex": 1, "crossref": 1, "newsapi": 1},
                "failed_sources": {},
                "output_target": "data/out.jsonl",
            },
            indent=indent,
        )


def test_cli_list_fetchers(monkeypatch, capsys) -> None:
    monkeypatch.setattr(sys, "argv", ["mdi-run", "list-fetchers"])
    main()
    out = capsys.readouterr().out
    assert "openalex" in out
    assert "crossref" in out
    assert "hacker-news" in out
    assert "website" in out
    assert "website_html" in out


def test_cli_run_success(tmp_path, monkeypatch, capsys) -> None:
    spec_file = tmp_path / "specs.json"
    spec_file.write_text(
        json.dumps(
            [{"source": "openalex", "config": {"query": "test", "max_pages": 1}}]
        ),
        encoding="utf-8",
    )

    def fake_run(*args, **kwargs):
        del args, kwargs
        return _DummySummary()

    monkeypatch.setattr("data_ingestion.cli.run_to_jsonl", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mdi-run",
            "run",
            "--spec-file",
            str(spec_file),
            "--output-file",
            str(tmp_path / "out.jsonl"),
            "--overwrite",
        ],
    )

    main()
    out = capsys.readouterr().out
    assert '"total_records": 3' in out
    assert '"newsapi": 1' in out


def test_cli_run_exits_on_missing_spec(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mdi-run",
            "run",
            "--spec-file",
            str(tmp_path / "nope.json"),
            "--output-file",
            str(tmp_path / "out.jsonl"),
        ],
    )
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code != 0
