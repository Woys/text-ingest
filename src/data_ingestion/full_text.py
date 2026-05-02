"""Full-text enrichment utilities for NormalizedRecord."""

from __future__ import annotations

import html
import json
import re
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from tempfile import SpooledTemporaryFile
from typing import TYPE_CHECKING, Any

import requests
from pypdf import PdfReader

from data_ingestion.config import FullTextResolutionConfig
from data_ingestion.http import SmartSession, build_retry_session
from data_ingestion.logging_utils import get_logger
from data_ingestion.models import FullTextDocument, NormalizedRecord

if TYPE_CHECKING:
    from collections.abc import Iterable

    from requests import Response

    from data_ingestion.http import _ManagedResponse

logger = get_logger(__name__)

_SCRIPT_STYLE_RE = re.compile(
    r"<(script|style)\b[^>]*>.*?</\1>",
    flags=re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


class FullTextResolver:
    """Populate NormalizedRecord.full_text from inline text or from full_text_url."""

    def __init__(self, config: FullTextResolutionConfig | None = None) -> None:
        self.config = config or FullTextResolutionConfig()
        self._thread_local = threading.local()

    def _get_session(self) -> SmartSession:
        session = getattr(self._thread_local, "session", None)
        if isinstance(session, SmartSession):
            return session

        session = build_retry_session(self.config.http)
        self._thread_local.session = session
        return session

    def enrich_record(self, record: NormalizedRecord) -> NormalizedRecord:
        """Return the same record with full_text populated when possible."""
        if self._clean_text(record.full_text):
            record.full_text = self._clean_text(record.full_text)
            return record

        if not record.full_text_url:
            return record

        session = self._get_session()

        try:
            with session.get(
                record.full_text_url,
                timeout=self.config.http.timeout_seconds,
                stream=True,
                headers={
                    "Accept": (
                        "application/pdf,"
                        "text/plain,text/html,application/xhtml+xml,"
                        "application/xml,application/json;q=0.9,*/*;q=0.8"
                    )
                },
            ) as response:
                response.raise_for_status()
                extracted = self._extract_text_from_response(response)
                if extracted:
                    record.full_text = extracted
                    if not record.full_text_url:
                        record.full_text_url = response.url
        except requests.RequestException as exc:
            logger.warning(
                "Full-text fetch failed source=%s external_id=%s url=%s error=%s",
                record.source,
                record.external_id,
                record.full_text_url,
                exc,
            )
        except Exception as exc:
            logger.warning(
                "Full-text extraction failed source=%s external_id=%s url=%s error=%s",
                record.source,
                record.external_id,
                record.full_text_url,
                exc,
            )

        return record

    def enrich_many(
        self,
        records: Iterable[NormalizedRecord],
    ) -> list[NormalizedRecord]:
        record_list = list(records)
        if not record_list:
            return []

        indexed_records = list(enumerate(record_list))
        results: dict[int, NormalizedRecord] = {}

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            pending_items = iter(indexed_records)
            futures: dict[Future[NormalizedRecord], int] = {}

            def submit_until_full() -> None:
                while len(futures) < self.config.max_workers:
                    try:
                        index, record = next(pending_items)
                    except StopIteration:
                        return
                    futures[executor.submit(self.enrich_record, record)] = index

            submit_until_full()

            while futures:
                done, _pending = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    index = futures.pop(future)
                    try:
                        results[index] = future.result()
                    except Exception as exc:
                        logger.warning(
                            "Concurrent full-text enrichment failed: %s",
                            exc,
                        )
                        results[index] = record_list[index]
                submit_until_full()

        return [results[i] for i in range(len(record_list))]

    def to_full_text_document(
        self,
        record: NormalizedRecord,
    ) -> FullTextDocument | None:
        text = self._clean_text(record.full_text)
        if not text:
            return None

        return FullTextDocument(
            source=record.source,
            external_id=record.external_id,
            title=record.title,
            url=record.url,
            full_text_url=record.full_text_url,
            full_text=text,
            content_type="enriched",
        )

    def _extract_text_from_response(
        self, response: Response | _ManagedResponse
    ) -> str | None:
        content_type = (response.headers.get("Content-Type") or "").lower()
        response_url = (response.url or "").lower()

        with self._download_to_spooled_file(response) as handle:
            if "application/pdf" in content_type or response_url.endswith(".pdf"):
                return self._pdf_to_text(handle)

            raw_text = self._read_text_file(handle, response.encoding)

            if "application/json" in content_type:
                return self._json_to_text(raw_text)

            if "text/html" in content_type or "application/xhtml+xml" in content_type:
                return self._html_to_text(raw_text)

            if "xml" in content_type or "text/plain" in content_type:
                return self._clean_text(raw_text)

            return self._clean_text(raw_text)

    def _download_to_spooled_file(
        self, response: Response | _ManagedResponse
    ) -> SpooledTemporaryFile[bytes]:
        handle: SpooledTemporaryFile[bytes] = SpooledTemporaryFile(  # noqa: SIM115
            max_size=self.config.spool_max_memory_bytes,
            mode="w+b",
        )
        total_bytes = 0

        for chunk in response.iter_content(chunk_size=self.config.download_chunk_size):
            if not chunk:
                continue
            total_bytes += len(chunk)
            if total_bytes > self.config.max_download_bytes:
                handle.close()
                raise ValueError(
                    f"Downloaded content exceeded "
                    f"max_download_bytes={self.config.max_download_bytes}"
                )
            handle.write(chunk)

        handle.seek(0)
        return handle

    def _read_text_file(
        self,
        handle: SpooledTemporaryFile[bytes],
        encoding: str | None,
    ) -> str:
        handle.seek(0)
        data = handle.read()
        handle.seek(0)

        if isinstance(data, str):
            return data

        return data.decode(encoding or "utf-8", errors="replace")

    def _pdf_to_text(self, handle: SpooledTemporaryFile[bytes]) -> str | None:
        handle.seek(0)

        try:
            reader = PdfReader(handle)
        except Exception as exc:
            logger.warning("Failed to open PDF for text extraction: %s", exc)
            return None

        pages: list[str] = []
        for page_number, page in enumerate(reader.pages, start=1):
            try:
                page_text = page.extract_text() or ""
            except Exception as exc:
                logger.warning(
                    "Failed to extract text from PDF page %d: %s",
                    page_number,
                    exc,
                )
                continue

            cleaned = self._clean_text(page_text)
            if cleaned:
                pages.append(cleaned)

        if not pages:
            return None

        return self._clean_text(" ".join(pages))

    def _html_to_text(self, raw_html: str) -> str | None:
        cleaned = _SCRIPT_STYLE_RE.sub(" ", raw_html)
        cleaned = _TAG_RE.sub(" ", cleaned)
        cleaned = html.unescape(cleaned)
        return self._clean_text(cleaned)

    def _json_to_text(self, raw_json: str) -> str | None:
        try:
            payload: Any = json.loads(raw_json)
        except json.JSONDecodeError:
            return self._clean_text(raw_json)

        candidates: list[str] = []

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    key_lower = key.lower()
                    if isinstance(child, str) and key_lower in {
                        "content",
                        "text",
                        "body",
                        "full_text",
                        "article",
                        "description",
                        "summary",
                    }:
                        candidates.append(child)
                    else:
                        walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        walk(payload)

        if not candidates:
            return None

        return self._clean_text(" ".join(candidates))

    def _clean_text(self, value: str | None) -> str | None:
        if not value:
            return None
        collapsed = _WS_RE.sub(" ", value).strip()
        if not collapsed:
            return None
        return collapsed[: self.config.max_chars]
