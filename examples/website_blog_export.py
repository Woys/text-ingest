"""Example: ingest multiple tech blogs via website feed autodiscovery."""

from __future__ import annotations

import os
from typing import Any

os.environ.setdefault("PYDANTIC_DISABLE_PLUGINS", "__all__")

from data_ingestion.pipeline import run_to_jsonl

OUTPUT_PATH = "data/website_blog.jsonl"
QUERY = None
START_DATE = None
END_DATE = None
MAX_ITEMS_PER_SITE = 50


WEBSITE_SOURCES: list[dict[str, str]] = [
    {"feed_url": "https://aws.amazon.com/blogs/aws/feed/"},
    {"feed_url": "https://developers.facebook.com/blog/feed/"},
    {
        "site_url": (
            "https://cloud.google.com/blog/topics/inside-google-cloud/"
            "whats-new-google-cloud"
        )
    },
    {"feed_url": "https://developers.googleblog.com/feed/"},
    {"site_url": "https://developers.google.com/workspace/release-notes"},
    {"feed_url": "https://ads-developers.googleblog.com/feeds/posts/default?alt=rss"},
    {"site_url": "https://developers.reddit.com/docs/blog"},
]


WEBSITE_HTML_SOURCES: list[dict[str, Any]] = [
    {
        "site_url": "https://ai.google.dev/gemini-api/docs/changelog",
    },
    {
        "site_url": "https://developers.google.com/google-ads/api/docs/release-notes",
    },
    {
        "site_url": "https://techcommunity.microsoft.com/category/bing/blog/adsapiblog",
    },
]


def with_filters(config: dict[str, Any]) -> dict[str, Any]:
    if QUERY is not None:
        config["query"] = QUERY
    if START_DATE is not None:
        config["start_date"] = START_DATE
    if END_DATE is not None:
        config["end_date"] = END_DATE
    return config


def main() -> None:
    fetcher_specs: list[dict[str, Any]] = [
        {
            "source": "website",
            "config": with_filters(
                {
                    **source_config,
                    "max_items": MAX_ITEMS_PER_SITE,
                }
            ),
        }
        for source_config in WEBSITE_SOURCES
    ]
    fetcher_specs.extend(
        [
            {
                "source": "website_html",
                "config": with_filters(
                    {
                        **source_config,
                        "max_items": MAX_ITEMS_PER_SITE,
                    }
                ),
            }
            for source_config in WEBSITE_HTML_SOURCES
        ]
    )

    summary = run_to_jsonl(
        fetcher_specs=fetcher_specs,
        output_file=OUTPUT_PATH,
        append=False,
        fail_fast=False,
        transform_spec={"transforms": [{"op": "dedupe", "keys": ["url"]}]},
    )
    print(summary.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
