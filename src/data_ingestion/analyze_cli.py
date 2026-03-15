from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from data_ingestion.analysis import analyze_topic_trends, search_industry_export


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mdi-analyze",
        description=(
            "Analyze trends and search text across industry export files (JSONL/CSV)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input-file", required=True, metavar="PATH")
    parser.add_argument("--topic", default=None, help="Topic phrase for trend/search")
    parser.add_argument("--text-query", default=None, help="Additional text query")
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Filter by source; can be repeated",
    )
    parser.add_argument("--start-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--lookback-days", type=int, default=90)
    parser.add_argument(
        "--limit", type=int, default=50, help="Max search matches returned"
    )
    parser.add_argument(
        "--exclude-raw-payload",
        action="store_true",
        help="Skip raw_payload fields during search",
    )
    parser.add_argument(
        "--output-matches-file",
        default=None,
        metavar="PATH",
        help="Optional JSONL file to write full matched rows",
    )
    return parser


def _preview(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": row.get("source"),
        "external_id": row.get("external_id")
        or row.get("id")
        or row.get("DOI")
        or row.get("objectID")
        or row.get("document_number"),
        "title": row.get("title") or row.get("story_title"),
        "published_date": row.get("published_date")
        or row.get("publication_date")
        or row.get("publishedAt")
        or row.get("created_at"),
        "topic": row.get("topic"),
        "url": row.get("url") or row.get("URL") or row.get("html_url"),
    }


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    args = _build_parser().parse_args()

    input_path = Path(args.input_file)
    if not input_path.is_file():
        print(f"error: input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    if not args.topic and not args.text_query:
        print("error: provide --topic and/or --text-query", file=sys.stderr)
        sys.exit(2)

    include_raw_payload = not args.exclude_raw_payload
    sources = args.source or None

    matches = search_industry_export(
        input_path,
        topic_query=args.topic,
        text_query=args.text_query,
        sources=sources,
        start_date=args.start_date,
        end_date=args.end_date,
        include_raw_payload=include_raw_payload,
        limit=args.limit,
    )

    trend_topic = args.topic or args.text_query
    assert trend_topic is not None

    trend = analyze_topic_trends(
        input_path,
        topic_query=trend_topic,
        text_query=args.text_query if args.topic else None,
        sources=sources,
        start_date=args.start_date,
        end_date=args.end_date,
        include_raw_payload=include_raw_payload,
        window_days=args.window_days,
        lookback_days=args.lookback_days,
    )

    if args.output_matches_file:
        _write_jsonl(Path(args.output_matches_file), matches)

    output = {
        "input_file": str(input_path),
        "topic": args.topic,
        "text_query": args.text_query,
        "sources": sources,
        "match_count": len(matches),
        "matches_preview": [_preview(row) for row in matches[:20]],
        "trend": trend,
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
