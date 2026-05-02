"""Crossref Works API fetcher."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from data_ingestion.config import CrossRefConfig
from data_ingestion.http import build_retry_session
from data_ingestion.logging_utils import get_logger
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = get_logger(__name__)


@register_fetcher("crossref")
class CrossRefFetcher(BaseFetcher):
    BASE_URL = "https://api.crossref.org/works"
    config_model = CrossRefConfig
    _TYPE_MAP: ClassVar[dict[str, RecordType]] = {
        "journal-article": RecordType.ARTICLE,
        "posted-content": RecordType.PREPRINT,
    }

    def __init__(self, config: CrossRefConfig) -> None:
        super().__init__(config)
        self.config: CrossRefConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "crossref"

    @staticmethod
    def _extract_topic(item: dict[str, Any]) -> str | None:
        subjects = item.get("subject") or []
        for subject in subjects:
            if isinstance(subject, str) and subject.strip():
                return subject.strip()
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        import re
        from datetime import date

        titles = item.get("title", [])
        title = titles[0] if titles else None
        authors = [
            f"{a.get('given', '')} {a.get('family', '')}".strip()
            for a in item.get("author", [])
            if a.get("family")
        ]

        pub_date = None
        date_parts = item.get("published", {}).get("date-parts", [])
        if date_parts and date_parts[0]:
            try:
                parts = date_parts[0]
                pub_date = date(
                    parts[0],
                    parts[1] if len(parts) > 1 else 1,
                    parts[2] if len(parts) > 2 else 1,
                )
            except Exception:
                pub_date = None

        ft_url = None
        for link in item.get("link") or []:
            if link.get("intended-application") in {
                "text-mining",
                "similarity-checking",
            }:
                ft_url = link.get("URL")
                break
            if not ft_url and link.get("URL"):
                ft_url = link.get("URL")

        url = item.get("URL") or item.get("resource", {}).get("primary", {}).get("URL")

        raw_abstract = item.get("abstract", "")
        abstract = (
            re.sub(r"<[^>]+>", "", raw_abstract).strip() if raw_abstract else None
        )

        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("DOI"),
            title=title,
            authors=authors,
            published_date=pub_date,
            url=url,
            abstract=abstract,
            full_text=None,
            full_text_url=ft_url or url,
            topic=self._extract_topic(item),
            record_type=self._TYPE_MAP.get(item.get("type", ""), RecordType.ARTICLE),
            raw_payload=item,
        )

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        base_params: dict[str, Any] = {"rows": self.config.rows}

        filter_parts: list[str] = []
        if self.config.date_mode == "publication":
            if self.config.start_date is not None:
                filter_parts.append(
                    f"from-pub-date:{self.config.start_date.isoformat()}"
                )
            if self.config.end_date is not None:
                filter_parts.append(
                    f"until-pub-date:{self.config.end_date.isoformat()}"
                )
        elif self.config.date_mode == "update":
            if self.config.start_date is not None:
                filter_parts.append(
                    f"from-update-date:{self.config.start_date.isoformat()}"
                )
            if self.config.end_date is not None:
                filter_parts.append(
                    f"until-update-date:{self.config.end_date.isoformat()}"
                )

        if filter_parts:
            base_params["filter"] = ",".join(filter_parts)
        if self.config.query:
            base_params["query"] = self.config.query
        if self.config.http.email:
            base_params["mailto"] = self.config.http.email

        pages_fetched = 0
        page_idx = 0
        while not self._page_limit_reached(pages_fetched):
            params = {**base_params, "offset": page_idx * self.config.rows}
            logger.info(
                "Crossref: requesting page=%d offset=%d rows=%d filters=%s",
                page_idx,
                params["offset"],
                self.config.rows,
                base_params.get("filter"),
            )
            res = self.session.get(
                self.BASE_URL,
                params=params,
                timeout=self.config.http.timeout_seconds,
            )
            res.raise_for_status()
            payload = res.json()

            items = payload.get("message", {}).get("items", [])
            if not items:
                logger.info(
                    "Crossref: no items page=%d offset=%d pages_fetched=%d",
                    page_idx,
                    params["offset"],
                    pages_fetched,
                )
                return

            total_results = payload.get("message", {}).get("total-results")
            logger.info(
                "Crossref: received page=%d items=%d total_results=%s",
                page_idx,
                len(items),
                total_results,
            )
            yield items
            pages_fetched += 1
            page_idx += 1
        logger.info("Crossref: stopped after max_pages pages_fetched=%d", pages_fetched)

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("language")
        if isinstance(raw, str):
            return raw
        return None
