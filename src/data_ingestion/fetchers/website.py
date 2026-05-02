"""Generic website RSS/Atom fetcher with optional feed autodiscovery."""

from __future__ import annotations

import contextlib
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin
from xml.etree import ElementTree

import requests

from data_ingestion.config import WebsiteConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.logging_utils import get_logger
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date

logger = get_logger(__name__)

RSS_ATOM_CONTENT_TYPES = (
    "application/rss+xml",
    "application/atom+xml",
    "application/xml",
    "text/xml",
)

DC_NS = "http://purl.org/dc/elements/1.1/"
CONTENT_NS = "http://purl.org/rss/1.0/modules/content/"
ATOM_NS = "http://www.w3.org/2005/Atom"


class _LinkDiscoveryParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.candidates: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "link":
            return

        attrs_map = {k.lower(): (v or "") for k, v in attrs}
        rel_values = {part.strip().lower() for part in attrs_map.get("rel", "").split()}
        href = attrs_map.get("href", "").strip()
        content_type = attrs_map.get("type", "").strip().lower()

        if "alternate" not in rel_values or not href:
            return
        if not content_type:
            return
        if any(ctype in content_type for ctype in RSS_ATOM_CONTENT_TYPES):
            self.candidates.append((href, content_type))


class _TextStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def get_text(self) -> str:
        return " ".join(" ".join(self.parts).split())


def _tag_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned if cleaned else None


def _extract_text(element: ElementTree.Element | None) -> str | None:
    if element is None:
        return None
    return _clean_text("".join(element.itertext()))


def _strip_html(value: str | None) -> str | None:
    if not value:
        return None
    parser = _TextStripper()
    with contextlib.suppress(Exception):
        parser.feed(value)
        parser.close()
    text = re.sub(r"\s+([,.;:!?])", r"\1", parser.get_text())
    return text or _clean_text(value)


@register_fetcher("website")
class WebsiteFetcher(BaseFetcher):
    """Fetches website news/articles from RSS or Atom feeds."""

    config_model = WebsiteConfig

    def __init__(self, config: WebsiteConfig) -> None:
        super().__init__(config)
        self.config: WebsiteConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "website"

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        if not raw:
            return None

        raw_value = raw.strip()
        if not raw_value:
            return None

        with contextlib.suppress(ValueError, TypeError):
            parsed = parsedate_to_datetime(raw_value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).date()

        with contextlib.suppress(ValueError):
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).date()

        return None

    def _resolve_feed_url(self) -> str:
        if self.config.feed_url:
            return self.config.feed_url

        site_url = self.config.site_url
        if site_url is None:
            raise FetcherError("Website fetcher requires 'feed_url' or 'site_url'.")

        try:
            response = self.session.get(
                site_url,
                timeout=self.config.http.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise FetcherError(f"Website autodiscovery request failed: {exc}") from exc

        parser = _LinkDiscoveryParser()
        try:
            parser.feed(response.text)
            parser.close()
        except Exception as exc:
            raise FetcherError(
                "Website autodiscovery failed while parsing HTML"
            ) from exc

        if not parser.candidates:
            raise FetcherError(
                f"No RSS/Atom feed link found in site HTML: {response.url}"
            )

        candidate_href, _content_type = parser.candidates[0]
        return urljoin(response.url, candidate_href)

    @staticmethod
    def _rss_item_to_dict(
        item: ElementTree.Element,
        *,
        feed_url: str,
        channel_title: str | None,
        channel_language: str | None,
    ) -> dict[str, Any]:
        categories = [_clean_text(cat.text) for cat in item.findall("category")]
        cleaned_categories = [cat for cat in categories if cat]

        author = _extract_text(item.find(f"{{{DC_NS}}}creator")) or _extract_text(
            item.find("author")
        )

        return {
            "feed_url": feed_url,
            "feed_title": channel_title,
            "language": channel_language,
            "title": _extract_text(item.find("title")),
            "link": _extract_text(item.find("link")),
            "guid": _extract_text(item.find("guid")),
            "author": author,
            "published": _extract_text(item.find("pubDate")),
            "description": _extract_text(item.find("description")),
            "content": _extract_text(item.find(f"{{{CONTENT_NS}}}encoded")),
            "categories": cleaned_categories,
            "raw_xml": ElementTree.tostring(item, encoding="unicode"),
        }

    @staticmethod
    def _atom_entry_to_dict(
        entry: ElementTree.Element,
        *,
        feed_url: str,
        channel_title: str | None,
        channel_language: str | None,
    ) -> dict[str, Any]:
        link = None
        for link_el in entry.findall(f"{{{ATOM_NS}}}link"):
            href = (link_el.get("href") or "").strip()
            if not href:
                continue
            rel = (link_el.get("rel") or "alternate").strip().lower()
            if rel == "alternate":
                link = href
                break
            if link is None:
                link = href

        categories: list[str] = []
        for category in entry.findall(f"{{{ATOM_NS}}}category"):
            term = _clean_text(category.get("term"))
            if term:
                categories.append(term)

        author = _extract_text(entry.find(f"{{{ATOM_NS}}}author/{{{ATOM_NS}}}name"))

        return {
            "feed_url": feed_url,
            "feed_title": channel_title,
            "language": _clean_text(
                entry.get("{http://www.w3.org/XML/1998/namespace}lang")
            )
            or channel_language,
            "title": _extract_text(entry.find(f"{{{ATOM_NS}}}title")),
            "link": link,
            "guid": _extract_text(entry.find(f"{{{ATOM_NS}}}id")),
            "author": author,
            "published": _extract_text(entry.find(f"{{{ATOM_NS}}}published"))
            or _extract_text(entry.find(f"{{{ATOM_NS}}}updated")),
            "description": _extract_text(entry.find(f"{{{ATOM_NS}}}summary")),
            "content": _extract_text(entry.find(f"{{{ATOM_NS}}}content")),
            "categories": categories,
            "raw_xml": ElementTree.tostring(entry, encoding="unicode"),
        }

    def _parse_feed_items(self, xml_text: str, feed_url: str) -> list[dict[str, Any]]:
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError as exc:
            raise FetcherError("Website feed returned invalid XML") from exc

        root_name = _tag_name(root.tag).lower()

        if root_name == "rss":
            channel = root.find("channel")
            if channel is None:
                return []
            channel_title = _extract_text(channel.find("title"))
            channel_language = _extract_text(channel.find("language"))
            items = channel.findall("item")
            return [
                self._rss_item_to_dict(
                    item,
                    feed_url=feed_url,
                    channel_title=channel_title,
                    channel_language=channel_language,
                )
                for item in items
            ]

        if root_name == "feed":
            channel_title = _extract_text(root.find(f"{{{ATOM_NS}}}title"))
            channel_language = _clean_text(
                root.get("{http://www.w3.org/XML/1998/namespace}lang")
            )
            entries = root.findall(f"{{{ATOM_NS}}}entry")
            return [
                self._atom_entry_to_dict(
                    entry,
                    feed_url=feed_url,
                    channel_title=channel_title,
                    channel_language=channel_language,
                )
                for entry in entries
            ]

        raise FetcherError(f"Unsupported feed format root tag: {root.tag}")

    def _matches_query(self, item: dict[str, Any]) -> bool:
        if not self.config.query:
            return True

        needle = self.config.query.casefold()
        haystack = " ".join(
            part
            for part in (
                _strip_html(item.get("title")),
                _strip_html(item.get("description")),
                _strip_html(item.get("content")),
            )
            if part
        ).casefold()
        return needle in haystack

    def _matches_date(self, item: dict[str, Any]) -> bool:
        published = self._parse_date(item.get("published"))
        if self.config.start_date is not None and (
            published is None or published < self.config.start_date
        ):
            return False
        return not (
            self.config.end_date is not None
            and (published is None or published > self.config.end_date)
        )

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        link = item.get("link")
        guid = item.get("guid")
        author = item.get("author")
        categories = item.get("categories") or []

        authors = [author.strip()] if isinstance(author, str) and author.strip() else []
        topic = categories[0] if categories else item.get("feed_title")

        abstract = _strip_html(item.get("description"))
        full_text = _strip_html(item.get("content")) or abstract

        return NormalizedRecord(
            source=self.source_name,
            external_id=guid or link,
            title=_strip_html(item.get("title")),
            authors=authors,
            published_date=self._parse_date(item.get("published")),
            url=link,
            abstract=abstract,
            full_text=full_text,
            full_text_url=link,
            topic=topic,
            record_type=RecordType.NEWS,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("language")
        if isinstance(raw, str):
            return raw
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        feed_url = self._resolve_feed_url()

        logger.info(
            "Website: requesting feed url=%s max_items=%d start_date=%s end_date=%s",
            feed_url,
            self.config.max_items,
            self.config.start_date,
            self.config.end_date,
        )
        try:
            response = self.session.get(
                feed_url,
                timeout=self.config.http.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise FetcherError(f"Website feed request failed: {exc}") from exc

        items = self._parse_feed_items(response.text, feed_url)
        if not items:
            logger.info("Website: feed had no entries (%s)", feed_url)
            return

        kept: list[dict[str, Any]] = []
        for item in items:
            if not self._matches_date(item):
                continue
            if not self._matches_query(item):
                continue
            kept.append(item)
            if len(kept) >= self.config.max_items:
                break

        if not kept:
            logger.info("Website: no entries matched filters (%s)", feed_url)
            return

        logger.info(
            "Website: received feed entries=%d kept=%d url=%s",
            len(items),
            len(kept),
            feed_url,
        )
        yield kept
