"""HTML website fetcher for pages without RSS/Atom feeds."""

from __future__ import annotations

import contextlib
import html
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlparse

import requests

from data_ingestion.config import WebsiteHtmlConfig
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


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attrs_map = {k.lower(): (v or "") for k, v in attrs}
        href = attrs_map.get("href", "").strip()
        if href:
            self.links.append(href)


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned if cleaned else None


def _strip_html(value: str | None) -> str | None:
    if not value:
        return None
    without_scripts = re.sub(
        r"(?is)<(script|style)[^>]*>.*?</\1>",
        " ",
        value,
    )
    no_tags = re.sub(r"(?is)<[^>]+>", " ", without_scripts)
    unescaped = html.unescape(no_tags)
    text = re.sub(r"\s+", " ", unescaped).strip()
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    return text or None


def _extract_html_lang(page_html: str) -> str | None:
    match = re.search(
        r'(?is)<html[^>]+lang\s*=\s*["\']([^"\']+)["\']',
        page_html,
    )
    if not match:
        return None
    return _clean_text(match.group(1))


@register_fetcher("website_html")
class WebsiteHtmlFetcher(BaseFetcher):
    """Fetches article pages from website HTML when feeds are unavailable."""

    config_model = WebsiteHtmlConfig

    def __init__(self, config: WebsiteHtmlConfig) -> None:
        super().__init__(config)
        self.config: WebsiteHtmlConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "website_html"

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

        date_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", raw_value)
        if date_match:
            with contextlib.suppress(ValueError):
                return datetime.strptime(date_match.group(1), "%Y-%m-%d").date()

        return None

    def _extract_meta(self, page_html: str, name: str) -> str | None:
        pattern = re.compile(
            rf"""(?is)<meta[^>]+(?:name|property)\s*=\s*["']{re.escape(name)}["'][^>]+content\s*=\s*["']([^"']+)["']""",
        )
        match = pattern.search(page_html)
        if match:
            return _clean_text(match.group(1))
        return None

    def _extract_title(self, page_html: str) -> str | None:
        for name in ("og:title", "twitter:title"):
            meta = self._extract_meta(page_html, name)
            if meta:
                return meta

        title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", page_html)
        if title_match:
            return _strip_html(title_match.group(1))
        return None

    def _extract_published_raw(self, page_html: str) -> str | None:
        for name in (
            "article:published_time",
            "article:modified_time",
            "date",
            "publish_date",
            "lastmod",
        ):
            meta = self._extract_meta(page_html, name)
            if meta:
                return meta

        time_match = re.search(
            r'(?is)<time[^>]+datetime\s*=\s*["\']([^"\']+)["\']',
            page_html,
        )
        if time_match:
            return _clean_text(time_match.group(1))
        return None

    def _extract_content(self, page_html: str) -> str | None:
        for name in ("description", "og:description", "twitter:description"):
            meta = self._extract_meta(page_html, name)
            if meta:
                return meta

        article_match = re.search(r"(?is)<article[^>]*>(.*?)</article>", page_html)
        if article_match:
            return _strip_html(article_match.group(1))

        body_match = re.search(r"(?is)<body[^>]*>(.*?)</body>", page_html)
        if body_match:
            return _strip_html(body_match.group(1))

        return _strip_html(page_html)

    def _extract_article_links(self, list_html: str, page_url: str) -> list[str]:
        parser = _AnchorParser()
        with contextlib.suppress(Exception):
            parser.feed(list_html)
            parser.close()

        base_netloc = urlparse(self.config.site_url).netloc
        include = self.config.link_include_patterns
        exclude = self.config.link_exclude_patterns

        results: list[str] = []
        seen: set[str] = set()
        for raw_link in parser.links:
            normalized = urljoin(page_url, raw_link)
            parsed = urlparse(normalized)

            if parsed.scheme not in {"http", "https"}:
                continue
            if parsed.netloc != base_netloc:
                continue

            normalized_no_fragment = normalized.split("#", 1)[0]
            path_and_query = (
                f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
            )

            if include and not any(pattern in path_and_query for pattern in include):
                continue
            if exclude and any(pattern in path_and_query for pattern in exclude):
                continue

            if normalized_no_fragment in seen:
                continue
            seen.add(normalized_no_fragment)
            results.append(normalized_no_fragment)

            if len(results) >= self.config.max_candidate_links:
                break

        return results

    def _matches_query(self, title: str | None, content: str | None) -> bool:
        if not self.config.query:
            return True

        needle = self.config.query.casefold()
        haystack = " ".join(part for part in (title, content) if part).casefold()
        return needle in haystack

    def _matches_date(self, published: date | None) -> bool:
        if self.config.start_date is not None and (
            published is None or published < self.config.start_date
        ):
            return False
        return not (
            self.config.end_date is not None
            and (published is None or published > self.config.end_date)
        )

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("url"),
            title=item.get("title"),
            authors=[],
            published_date=self._parse_date(item.get("published_raw")),
            url=item.get("url"),
            abstract=item.get("summary"),
            full_text=item.get("content"),
            full_text_url=item.get("url"),
            topic=urlparse(self.config.site_url).netloc,
            record_type=RecordType.NEWS,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("language")
        if isinstance(raw, str):
            return raw
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        list_pages = self.config.list_page_urls or [self.config.site_url]

        article_links: list[str] = []
        seen_links: set[str] = set()
        for list_page in list_pages:
            try:
                response = self.session.get(
                    list_page,
                    timeout=self.config.http.timeout_seconds,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                raise FetcherError(
                    f"Website HTML list-page request failed ({list_page}): {exc}"
                ) from exc

            for link in self._extract_article_links(response.text, response.url):
                if link in seen_links:
                    continue
                seen_links.add(link)
                article_links.append(link)
                if len(article_links) >= self.config.max_candidate_links:
                    break
            if len(article_links) >= self.config.max_candidate_links:
                break

        if not article_links:
            if self.config.include_list_pages_as_items:
                logger.info(
                    "Website HTML: no candidate links found, using list pages directly"
                )
                article_links = list_pages.copy()
            else:
                logger.info("Website HTML: no candidate article links found")
                return

        kept: list[dict[str, Any]] = []
        for url in article_links:
            try:
                response = self.session.get(
                    url,
                    timeout=self.config.http.timeout_seconds,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.warning(
                    "Website HTML: failed to fetch article url=%s err=%s",
                    url,
                    exc,
                )
                continue

            title = self._extract_title(response.text)
            content = self._extract_content(response.text)
            published_raw = self._extract_published_raw(response.text)
            published = self._parse_date(published_raw)

            if not self._matches_date(published):
                continue
            if not self._matches_query(title, content):
                continue

            summary = content[:500] if content else None
            kept.append(
                {
                    "url": response.url,
                    "title": title,
                    "language": _extract_html_lang(response.text),
                    "published_raw": published_raw,
                    "summary": summary,
                    "content": content,
                    "source_list_url": list_pages[0],
                }
            )
            if len(kept) >= self.config.max_items:
                break

        if not kept:
            logger.info("Website HTML: no pages matched filters")
            return

        yield kept
