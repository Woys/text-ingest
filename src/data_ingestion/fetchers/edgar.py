"""SEC EDGAR fetcher."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import requests

from data_ingestion.config import EdgarConfig
from data_ingestion.exceptions import FetcherError
from data_ingestion.http import build_retry_session
from data_ingestion.models import NormalizedRecord, RecordType
from data_ingestion.registry import register_fetcher

from .base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date


@register_fetcher("edgar")
class EdgarFetcher(BaseFetcher):
    """Fetches SEC EDGAR submissions using the EFTS API."""

    BASE_URL = "https://efts.sec.gov/LATEST/search-index"
    config_model = EdgarConfig

    def __init__(self, config: EdgarConfig) -> None:
        super().__init__(config)
        self.config: EdgarConfig = config
        self.session = build_retry_session(config.http)

    @property
    def source_name(self) -> str:
        return "edgar"

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        from datetime import date

        if not raw:
            return None
        with contextlib.suppress(ValueError):
            return date.fromisoformat(raw)
        return None

    def normalize(self, item: dict[str, Any]) -> NormalizedRecord:
        source_data = item.get("_source", {})
        id_val = item.get("_id", "")

        parts = id_val.split(":", 1)
        adsh = parts[0] if len(parts) > 0 else ""
        filename = parts[1] if len(parts) > 1 else ""

        adsh_clean = adsh.replace("-", "")

        ciks = source_data.get("ciks", [])
        cik = ciks[0] if ciks else ""

        ft_url = None
        if cik and adsh_clean and filename:
            # Note: CIK can be used as is, leading zeroes are fine for the
            # SEC Archives URL but traditionally it's stripped. It works
            # either way. We strip it just in case.
            cik_clean = cik.lstrip("0") or "0"
            ft_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik_clean}/{adsh_clean}/{filename}"
            )

        authors = source_data.get("display_names", [])
        forms = source_data.get("root_forms", [])
        form = forms[0] if forms else "Filing"
        title = f"{form} - {authors[0]}" if authors else form

        return NormalizedRecord(
            source=self.source_name,
            external_id=id_val,
            title=title,
            authors=authors,
            published_date=self._parse_date(source_data.get("file_date")),
            url=ft_url,
            abstract=None,
            full_text=None,
            full_text_url=ft_url,
            topic=form,
            record_type=RecordType.ARTICLE,
            raw_payload=item,
        )

    def extract_language(self, item: dict[str, Any]) -> str | None:
        del item
        return "en"

    def fetch_pages(self) -> Iterator[list[dict[str, Any]]]:
        pages_fetched = 0
        page = 1
        while not self._page_limit_reached(pages_fetched):
            params: dict[str, Any] = {
                "q": self.config.query or "",
                "from": (page - 1) * self.config.per_page,
                "size": self.config.per_page,
            }

            if self.config.start_date is not None:
                params["startdt"] = self.config.start_date.isoformat()
            if self.config.end_date is not None:
                params["enddt"] = self.config.end_date.isoformat()

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
                    f"Edgar request failed on page {page}: {exc}"
                ) from exc
            except ValueError as exc:
                raise FetcherError(
                    f"Edgar returned invalid JSON on page {page}"
                ) from exc

            hits: list[dict[str, Any]] = payload.get("hits", {}).get("hits", [])
            if not hits:
                return

            yield hits
            pages_fetched += 1

            if len(hits) < self.config.per_page:
                return
            page += 1
