"""Example: analyze emerging trends + text search on full industry export."""

from __future__ import annotations

import json

from data_ingestion.analysis import analyze_topic_trends, search_industry_export

INPUT_FILE = "data/all_sources_ingestion.csv"
TOPIC = "data engineering"
TEXT_QUERY = "agent"


def main() -> None:
    matches = search_industry_export(
        INPUT_FILE,
        topic_query=TOPIC,
        text_query=TEXT_QUERY,
        limit=25,
    )

    trend = analyze_topic_trends(
        INPUT_FILE,
        topic_query=TOPIC,
        text_query=TEXT_QUERY,
        lookback_days=60,
        window_days=7,
    )

    print(f"Matched records: {len(matches)}")
    print("\nTrend summary:")
    print(json.dumps(trend, indent=2))

    print("\nTop 5 matches:")
    for row in matches[:5]:
        print(
            json.dumps(
                {
                    "source": row.get("source"),
                    "title": row.get("title") or row.get("story_title"),
                    "topic": row.get("topic"),
                    "published_date": row.get("published_date")
                    or row.get("publication_date")
                    or row.get("publishedAt"),
                    "url": row.get("url") or row.get("URL") or row.get("html_url"),
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
