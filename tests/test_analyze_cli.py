from __future__ import annotations

import json
import sys

from data_ingestion.analyze_cli import main


def test_analyze_cli_outputs_summary(tmp_path, monkeypatch, capsys) -> None:
    input_path = tmp_path / "records.jsonl"
    input_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "source": "openalex",
                        "title": "Data engineering platform",
                        "topic": "data engineering",
                        "published_date": "2026-03-14",
                        "url": "https://example.com/1",
                        "raw_payload": {"id": "1"},
                    }
                ),
                json.dumps(
                    {
                        "source": "crossref",
                        "title": "AI ops update",
                        "topic": "ai",
                        "published_date": "2026-03-13",
                        "url": "https://example.com/2",
                        "raw_payload": {"id": "2"},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mdi-analyze",
            "--input-file",
            str(input_path),
            "--topic",
            "data engineering",
            "--text-query",
            "platform",
            "--lookback-days",
            "30",
            "--window-days",
            "7",
        ],
    )

    main()
    out = capsys.readouterr().out
    payload = json.loads(out)

    assert payload["topic"] == "data engineering"
    assert payload["match_count"] >= 1
    assert "trend" in payload
