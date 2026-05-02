"""Wikipedia REST/API fetcher."""

from __future__ import annotations

import contextlib
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

import requests

from data_ingestion.config import WikipediaConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date


@register_fetcher("wikipedia")
class WikipediaFetcher(BaseFetcher):
    """Fetches topic summaries from Wikipedia search results."""

    BASE_URL = "https://{lang}.wikipedia.org/w/api.php"
    SUMMARY_URL = "https://{lang}.wikipedia.org/api/rest_v1/page/summary/{title}"
    config_model = WikipediaConfig

    def __init__(self, config: WikipediaConfig) -> None:
        super().__init__(config)
        self.config: WikipediaConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "wikipedia"

    def _language(self) -> str:
        if self.config.languages:
            return self.config.languages[0]
        return self.config.wiki_language

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        from datetime import datetime, timezone

        if not raw:
            return None
        with contextlib.suppress(ValueError):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).date()
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        title = item.get("title")
        pageid = item.get("pageid")
        url = item.get("url")
        summary = item.get("extract")
        raw_snippet = item.get("snippet")
        snippet = re.sub(r"<[^>]+>", "", raw_snippet).strip() if raw_snippet else None

        return NormalizedRecord(
            source=self.source_name,
            external_id=str(pageid) if pageid is not None else url,
            title=title,
            authors=[],
            published_date=self._parse_date(item.get("timestamp")),
            url=url,
            abstract=summary or snippet,
            full_text=summary,
            full_text_url=url,
            topic=self.config.query,
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("lang")
        if isinstance(raw, str):
            return raw
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        language = self._language()
        url = self.BASE_URL.format(lang=language)
        page_size = min(self.config.page_size, 50)

        pages_fetched = 0
        page = 0
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "action": "query",
                "list": "search",
                "format": "json",
                "srlimit": page_size,
                "srsearch": self.config.query,
                "sroffset": page * page_size,
            }

            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.http.timeout_seconds,
                )
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
            except requests.RequestException as exc:
                raise FetcherError(
                    f"Wikipedia request failed on page {page + 1}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"Wikipedia returned invalid JSON on page {page + 1}"
                ) from exc

            results: list[dict[str, Any]] = payload.get("query", {}).get("search", [])
            if not results:
                return

            enriched: list[dict[str, Any]] = []
            for item in results:
                title = item.get("title")
                summary = None
                page_url = None
                if isinstance(title, str) and title:
                    try:
                        summary_response = self.session.get(
                            self.SUMMARY_URL.format(
                                lang=language,
                                title=quote_plus(title),
                            ),
                            timeout=self.config.http.timeout_seconds,
                        )
                        summary_response.raise_for_status()
                        summary_payload: dict[str, Any] = summary_response.json()
                        summary = summary_payload.get("extract")
                        page_url = (
                            summary_payload.get("content_urls", {})
                            .get("desktop", {})
                            .get("page")
                        )
                    except Exception:
                        summary = None
                        page_url = None

                enriched.append(
                    {
                        **item,
                        "lang": language,
                        "extract": summary,
                        "url": page_url
                        or (
                            f"https://{language}.wikipedia.org/?curid={item.get('pageid')}"
                        ),
                    }
                )

            yield enriched
            pages_fetched += 1
            page += 1
