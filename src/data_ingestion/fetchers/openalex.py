"""OpenAlex Works API fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from data_ingestion.config import OpenAlexConfig
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


@register_fetcher("openalex")
class OpenAlexFetcher(BaseFetcher):
    BASE_URL = "https://api.openalex.org/works"
    config_model = OpenAlexConfig

    def __init__(self, config: OpenAlexConfig) -> None:
        super().__init__(config)
        self.config: OpenAlexConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "openalex"

    @staticmethod
    def _reconstruct_abstract(
        inverted_index: dict[str, list[int]] | None,
    ) -> str | None:
        if not inverted_index:
            return None
        position_word = {
            pos: word for word, positions in inverted_index.items() for pos in positions
        }
        return " ".join(position_word[p] for p in sorted(position_word))

    @staticmethod
    def _extract_topic(item: dict[str, Any]) -> str | None:
        primary_topic = item.get("primary_topic") or {}
        topic = primary_topic.get("display_name")
        if isinstance(topic, str) and topic.strip():
            return topic.strip()

        for concept in item.get("concepts") or []:
            name = concept.get("display_name")
            if isinstance(name, str) and name.strip():
                return name.strip()
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        from datetime import date

        pub_date: date | None = None
        if raw_date := item.get("publication_date"):
            with contextlib.suppress(ValueError):
                pub_date = date.fromisoformat(raw_date)

        authors = [
            a["author"]["display_name"]
            for a in item.get("authorships", [])
            if a.get("author", {}).get("display_name")
        ]

        oa_info = item.get("open_access") or {}
        best_oa_location = item.get("best_oa_location") or {}
        pdf_url = oa_info.get("oa_url") or best_oa_location.get("pdf_url")
        landing_page = (
            best_oa_location.get("landing_page_url")
            or item.get("doi")
            or item.get("id")
        )

        return NormalizedRecord(
            source=self.source_name,
            external_id=item.get("id"),
            title=item.get("title"),
            authors=authors,
            published_date=pub_date,
            url=landing_page,
            abstract=self._reconstruct_abstract(item.get("abstract_inverted_index")),
            full_text=None,
            full_text_url=pdf_url or landing_page,
            topic=self._extract_topic(item),
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("language")
        if isinstance(raw, str):
            return raw
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        params: dict[str, Any] = {
            "per-page": self.config.per_page,
            "cursor": "*",
        }

        filter_parts: list[str] = []

        if self.config.start_date is not None:
            filter_parts.append(
                f"from_publication_date:{self.config.start_date.isoformat()}"
            )
        if self.config.end_date is not None:
            filter_parts.append(
                f"to_publication_date:{self.config.end_date.isoformat()}"
            )

        if self.config.query and self.config.search_mode in {"exact", "broad"}:
            params["search"] = self.config.query

        if filter_parts:
            params["filter"] = ",".join(filter_parts)

        if self.config.http.email:
            params["mailto"] = self.config.http.email

        pages_fetched = 0
        while not self._page_limit_reached(pages_fetched):
            logger.info(
                "OpenAlex: requesting page=%d per_page=%d cursor=%s filters=%s",
                pages_fetched,
                self.config.per_page,
                str(params.get("cursor", ""))[:24],
                params.get("filter"),
            )
            try:
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.config.http.timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                raise FetcherError(f"OpenAlex fail: {exc}") from exc

            results = payload.get("results", [])
            if not results:
                logger.info(
                    "OpenAlex: no results page=%d pages_fetched=%d",
                    pages_fetched,
                    pages_fetched,
                )
                return

            meta = payload.get("meta", {})
            logger.info(
                "OpenAlex: received page=%d results=%d count=%s",
                pages_fetched,
                len(results),
                meta.get("count"),
            )
            yield results
            pages_fetched += 1

            next_cursor = payload.get("meta", {}).get("next_cursor")
            if not next_cursor:
                logger.info(
                    "OpenAlex: no next cursor after pages_fetched=%d",
                    pages_fetched,
                )
                return
            params["cursor"] = next_cursor
        logger.info("OpenAlex: stopped after max_pages pages_fetched=%d", pages_fetched)
