from __future__ import annotations

import json
from datetime import date

from data_ingestion.analysis import (
    analyze_topic_trends,
    iter_export_rows,
    search_industry_export,
)


def test_iter_export_rows_jsonl(tmp_path) -> None:
    path = tmp_path / "records.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"source": "openalex", "title": "A"}),
                json.dumps({"source": "crossref", "title": "B"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rows = list(iter_export_rows(path))
    assert len(rows) == 2
    assert rows[0]["source"] == "openalex"
    assert rows[1]["title"] == "B"


def test_search_industry_export_covers_raw_payload_csv(tmp_path) -> None:
    path = tmp_path / "records.csv"
    path.write_text(
        "source,title,published_date,raw_payload\n"
        "openalex,AI systems,2026-03-14,"
        '"{""notes"": ""pharma industry trend spike""}"\n',
        encoding="utf-8",
    )

    rows = search_industry_export(
        path,
        topic_query="pharma",
        text_query="trend spike",
        limit=10,
    )
    assert len(rows) == 1
    assert rows[0]["source"] == "openalex"


def test_analyze_topic_trends_growth(tmp_path) -> None:
    path = tmp_path / "records.jsonl"
    rows = [
        {
            "source": "openalex",
            "title": "Agentic workflow for data engineering",
            "topic": "data engineering",
            "published_date": "2026-03-14",
            "raw_payload": {"id": "1"},
        },
        {
            "source": "crossref",
            "title": "Data engineering platform",
            "topic": "data engineering",
            "published_date": "2026-03-13",
            "raw_payload": {"id": "2"},
        },
        {
            "source": "newsapi",
            "title": "Cloud infra overview",
            "topic": "cloud",
            "published_date": "2026-03-10",
            "raw_payload": {"id": "3"},
        },
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")

    trend = analyze_topic_trends(
        path,
        topic_query="data engineering",
        text_query="platform",
        lookback_days=7,
        window_days=3,
        reference_date=date(2026, 3, 15),
    )

    assert trend["topic_query"] == "data engineering"
    assert trend["matched_records"] == 1
    assert trend["recent_window"]["count"] == 1
    assert trend["trend_status"] in {"emerging", "up", "flat", "down"}
    assert isinstance(trend["daily_counts"], list)
    assert len(trend["daily_counts"]) == 7
