"""GitHub repository search fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import GitHubConfig
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


@register_fetcher("github")
class GitHubFetcher(BaseFetcher):
    """Fetches public repository metadata from GitHub Search API."""

    BASE_URL = "https://api.github.com/search/repositories"
    config_model = GitHubConfig

    def __init__(self, config: GitHubConfig) -> None:
        super().__init__(config)
        self.config: GitHubConfig = config
        self.session = build_retry_session(config.http)
        if self.config.github_token:
            self.session.headers.update(
                {"Authorization": f"Bearer {self.config.github_token}"}
            )

    @property
    def source_name(self) -> str:
        return "github"

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
        owner_login = item.get("owner", {}).get("login")
        authors = [owner_login] if owner_login else []

        return NormalizedRecord(
            source=self.source_name,
            external_id=str(item.get("id")) if item.get("id") is not None else None,
            title=item.get("full_name"),
            authors=authors,
            published_date=self._parse_date(item.get("updated_at")),
            url=item.get("html_url"),
            abstract=item.get("description"),
            full_text=None,
            full_text_url=item.get("html_url"),
            topic=item.get("language"),
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        raw = item.get("human_language")
        if isinstance(raw, str):
            return raw
        return None

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        pages_fetched = 0
        page = 1
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "q": self.config.query,
                "sort": self.config.sort,
                "order": "desc",
                "per_page": self.config.per_page,
                "page": page,
            }

            logger.info(
                "GitHub: requesting page=%d per_page=%d sort=%s",
                page,
                self.config.per_page,
                self.config.sort,
            )
            try:
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.config.http.timeout_seconds,
                )
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
            except requests.RequestException as exc:
                raise FetcherError(
                    f"GitHub request failed on page {page}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"GitHub returned invalid JSON on page {page}"
                ) from exc

            items: list[dict[str, Any]] = payload.get("items", [])
            if not items:
                logger.info(
                    "GitHub: no items page=%d pages_fetched=%d",
                    page,
                    pages_fetched,
                )
                return

            logger.info(
                "GitHub: received page=%d items=%d total_count=%s",
                page,
                len(items),
                payload.get("total_count"),
            )
            yield items
            pages_fetched += 1

            if len(items) < self.config.per_page:
                logger.info(
                    "GitHub: partial page page=%d items=%d pages_fetched=%d",
                    page,
                    len(items),
                    pages_fetched,
                )
                return
            page += 1
        logger.info("GitHub: stopped after max_pages pages_fetched=%d", pages_fetched)
