"""Google News RSS fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus
from xml.etree import ElementTree

import requests

from data_ingestion.config import GoogleNewsConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date


@register_fetcher("googlenews")
class GoogleNewsFetcher(BaseFetcher):
    """Fetches news items from Google News RSS search."""

    BASE_URL = "https://news.google.com/rss/search"
    config_model = GoogleNewsConfig

    def __init__(self, config: GoogleNewsConfig) -> None:
        super().__init__(config)
        self.config: GoogleNewsConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "googlenews"

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        from email.utils import parsedate_to_datetime

        if not raw:
            return None
        with contextlib.suppress(ValueError, TypeError):
            return parsedate_to_datetime(raw).date()
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("guid") or item.get("link"),
            title=item.get("title"),
            authors=[],
            published_date=self._parse_date(item.get("pubDate")),
            url=item.get("link"),
            abstract=item.get("description"),
            full_text=None,
            full_text_url=item.get("link"),
            topic=self.config.query,
            record_type=RecordType.NEWS,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("language")
        if isinstance(raw, str):
            return raw
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        query = quote_plus(self.config.query or "")
        url = (
            f"{self.BASE_URL}?q={query}&hl={self.config.hl}&gl={self.config.gl}"
            f"&ceid={self.config.ceid}"
        )

        try:
            response = self.session.get(
                url,
                timeout=self.config.http.timeout_seconds,
            )
            response.raise_for_status()
            root = ElementTree.fromstring(response.text)
        except requests.RequestException as exc:
            raise FetcherError(f"GoogleNews request failed: {exc}") from exc
        except ElementTree.ParseError as exc:
            raise FetcherError("GoogleNews returned invalid XML") from exc

        channel = root.find("channel")
        channel_language = channel.findtext("language") if channel is not None else None
        item_elements = root.findall("./channel/item")
        if not item_elements:
            return

        items: list[dict[str, Any]] = []
        for element in item_elements[: self.config.page_size]:
            items.append(
                {
                    "title": element.findtext("title"),
                    "link": element.findtext("link"),
                    "guid": element.findtext("guid"),
                    "pubDate": element.findtext("pubDate"),
                    "description": element.findtext("description"),
                    "language": channel_language or self.config.hl,
                }
            )

        if items:
            yield items
