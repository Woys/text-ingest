"""Example: ingest external web sources with dlt.

Run:
    pip install dlt requests
    python examples/dlt_source_ingestion.py
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus
from xml.etree import ElementTree

import requests

USER_AGENT = "text-ingest-dlt/0.3.0"
MAX_ITEMS = 25


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def wikipedia_summaries(titles: list[str]) -> list[dict[str, Any]]:
    session = _session()
    rows: list[dict[str, Any]] = []
    for title in titles:
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote_plus(title)}"
        response = session.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        rows.append(
            {
                "source": "wikipedia",
                "title": payload.get("title"),
                "summary": payload.get("extract"),
                "url": payload.get("content_urls", {}).get("desktop", {}).get("page"),
                "lang": payload.get("lang") or "en",
                "fetched_at": _now_iso(),
                "raw_payload": payload,
            }
        )
    return rows


def reddit_posts(subreddit: str, limit: int = MAX_ITEMS) -> list[dict[str, Any]]:
    session = _session()
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    rows: list[dict[str, Any]] = []
    for child in payload.get("data", {}).get("children", []):
        item = child.get("data", {})
        rows.append(
            {
                "source": "reddit",
                "subreddit": subreddit,
                "title": item.get("title"),
                "body": item.get("selftext"),
                "url": f"https://www.reddit.com{item.get('permalink', '')}",
                "score": item.get("score"),
                "num_comments": item.get("num_comments"),
                "created_utc": item.get("created_utc"),
                "lang": "en",
                "fetched_at": _now_iso(),
                "raw_payload": item,
            }
        )
    return rows


def github_repos(query: str, per_page: int = MAX_ITEMS) -> list[dict[str, Any]]:
    session = _session()
    url = "https://api.github.com/search/repositories"
    response = session.get(
        url,
        params={"q": query, "sort": "updated", "order": "desc", "per_page": per_page},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    rows: list[dict[str, Any]] = []
    for repo in payload.get("items", []):
        rows.append(
            {
                "source": "github",
                "name": repo.get("full_name"),
                "description": repo.get("description"),
                "url": repo.get("html_url"),
                "stars": repo.get("stargazers_count"),
                "forks": repo.get("forks_count"),
                "language": repo.get("language"),
                "updated_at": repo.get("updated_at"),
                "fetched_at": _now_iso(),
                "raw_payload": repo,
            }
        )
    return rows


def stackexchange_questions(
    tag: str, pagesize: int = MAX_ITEMS
) -> list[dict[str, Any]]:
    session = _session()
    url = "https://api.stackexchange.com/2.3/questions"
    response = session.get(
        url,
        params={
            "site": "stackoverflow",
            "order": "desc",
            "sort": "activity",
            "tagged": tag,
            "pagesize": pagesize,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    rows: list[dict[str, Any]] = []
    for item in payload.get("items", []):
        rows.append(
            {
                "source": "stackexchange",
                "site": "stackoverflow",
                "title": item.get("title"),
                "url": item.get("link"),
                "score": item.get("score"),
                "answer_count": item.get("answer_count"),
                "tags": item.get("tags"),
                "creation_date": item.get("creation_date"),
                "last_activity_date": item.get("last_activity_date"),
                "fetched_at": _now_iso(),
                "raw_payload": item,
            }
        )
    return rows


def open_library_books(query: str, limit: int = MAX_ITEMS) -> list[dict[str, Any]]:
    session = _session()
    url = "https://openlibrary.org/search.json"
    response = session.get(url, params={"q": query, "limit": limit}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    rows: list[dict[str, Any]] = []
    for doc in payload.get("docs", []):
        key = _safe_text(doc.get("key"))
        rows.append(
            {
                "source": "openlibrary",
                "title": _safe_text(doc.get("title")),
                "author_name": doc.get("author_name", []),
                "first_publish_year": doc.get("first_publish_year"),
                "language": doc.get("language", []),
                "edition_count": doc.get("edition_count"),
                "url": f"https://openlibrary.org{key}" if key else None,
                "fetched_at": _now_iso(),
                "raw_payload": doc,
            }
        )
    return rows


def google_news_rss(query: str, limit: int = MAX_ITEMS) -> list[dict[str, Any]]:
    session = _session()
    rss_url = (
        "https://news.google.com/rss/search"
        f"?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    response = session.get(rss_url, timeout=30)
    response.raise_for_status()
    root = ElementTree.fromstring(response.text)
    rows: list[dict[str, Any]] = []
    for item in root.findall("./channel/item")[:limit]:
        rows.append(
            {
                "source": "google_news_rss",
                "title": _safe_text(item.findtext("title")),
                "url": _safe_text(item.findtext("link")),
                "published_at": _safe_text(item.findtext("pubDate")),
                "description": _safe_text(item.findtext("description")),
                "fetched_at": _now_iso(),
                "raw_payload": ElementTree.tostring(item, encoding="unicode"),
            }
        )
    return rows


def guardian_content(query: str, limit: int = MAX_ITEMS) -> list[dict[str, Any]]:
    session = _session()
    url = "https://content.guardianapis.com/search"
    response = session.get(
        url,
        params={"q": query, "page-size": limit, "api-key": "test"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    rows: list[dict[str, Any]] = []
    for item in payload.get("response", {}).get("results", []):
        rows.append(
            {
                "source": "guardian",
                "title": item.get("webTitle"),
                "url": item.get("webUrl"),
                "section": item.get("sectionName"),
                "published_at": item.get("webPublicationDate"),
                "fetched_at": _now_iso(),
                "raw_payload": item,
            }
        )
    return rows


def run_dlt() -> None:
    try:
        import dlt
    except ImportError as exc:  # pragma: no cover - runtime guidance
        raise RuntimeError(
            "dlt is not installed. Install with: pip install dlt"
        ) from exc

    pipeline = dlt.pipeline(
        pipeline_name="external_sources_pipeline",
        destination="duckdb",
        dataset_name="external_sources",
    )

    load_info = pipeline.run(wikipedia_summaries(["cloud computing", "generative AI"]))
    print("Loaded wikipedia:", load_info)

    load_info = pipeline.run(reddit_posts("smallbusiness"))
    print("Loaded reddit:", load_info)

    load_info = pipeline.run(github_repos("data engineering language:python"))
    print("Loaded github:", load_info)

    load_info = pipeline.run(stackexchange_questions("python"))
    print("Loaded stackexchange:", load_info)

    load_info = pipeline.run(open_library_books("machine learning"))
    print("Loaded openlibrary:", load_info)

    load_info = pipeline.run(google_news_rss("AI infrastructure"))
    print("Loaded google news rss:", load_info)

    load_info = pipeline.run(guardian_content("artificial intelligence"))
    print("Loaded guardian:", load_info)


if __name__ == "__main__":
    run_dlt()
