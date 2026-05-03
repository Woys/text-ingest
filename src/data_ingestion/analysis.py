from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

_TEXT_FIELDS = (
    "source",
    "external_id",
    "title",
    "authors",
    "published_date",
    "url",
    "abstract",
    "full_text",
    "full_text_url",
    "topic",
    "record_type",
)

_DATE_FIELDS = (
    "published_date",
    "publication_date",
    "publishedAt",
    "created_at",
    "fetched_at",
)

_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
}


def _parse_date_value(value: Any) -> date | None:
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    if len(raw) >= 10:
        candidate = raw[:10]
        try:
            return date.fromisoformat(candidate)
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _row_record_date(row: dict[str, Any]) -> date | None:
    for key in _DATE_FIELDS:
        parsed = _parse_date_value(row.get(key))
        if parsed is not None:
            return parsed

    raw_payload = row.get("raw_payload")
    if isinstance(raw_payload, dict):
        for key in _DATE_FIELDS:
            parsed = _parse_date_value(raw_payload.get(key))
            if parsed is not None:
                return parsed

    return None


def _iter_text_values(value: Any) -> Iterator[str]:
    if value is None:
        return

    if isinstance(value, str):
        if value.strip():
            yield value
        return

    if isinstance(value, (int, float, bool)):
        yield str(value)
        return

    if isinstance(value, dict):
        for sub_value in value.values():
            yield from _iter_text_values(sub_value)
        return

    if isinstance(value, list):
        for item in value:
            yield from _iter_text_values(item)


def _row_search_text(row: dict[str, Any], *, include_raw_payload: bool) -> str:
    pieces: list[str] = []

    for key in _TEXT_FIELDS:
        pieces.extend(_iter_text_values(row.get(key)))

    if include_raw_payload:
        pieces.extend(_iter_text_values(row.get("raw_payload")))

    return " ".join(pieces).lower()


def _maybe_decode_json(value: str) -> Any:
    raw = value.strip()
    if not raw:
        return None

    if not (
        (raw.startswith("{") and raw.endswith("}"))
        or (raw.startswith("[") and raw.endswith("]"))
    ):
        return value

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return value


def _normalize_csv_row(row: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if value == "":
            out[key] = None
            continue

        if key in {"authors", "raw_payload"}:
            decoded = _maybe_decode_json(value)
            out[key] = decoded
            continue

        out[key] = value

    return out


def iter_export_rows(input_file: str | Path) -> Iterator[dict[str, Any]]:
    """Iterate export rows from JSONL or CSV outputs."""
    path = Path(input_file)

    if not path.is_file():
        raise FileNotFoundError(f"export file not found: {path}")

    suffix = path.suffix.lower()

    if suffix == ".jsonl":
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                raw = line.strip()
                if not raw:
                    continue
                yield json.loads(raw)
        return

    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                yield _normalize_csv_row(row)
        return

    raise ValueError("unsupported export format; expected .jsonl or .csv")


def search_industry_export(
    input_file: str | Path,
    *,
    topic_query: str | None = None,
    text_query: str | None = None,
    sources: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    include_raw_payload: bool = True,
    limit: int | None = 200,
) -> list[dict[str, Any]]:
    """Search across full industry export text (including raw payload by default)."""
    topic_q = (topic_query or "").strip().lower()
    text_q = (text_query or "").strip().lower()

    if not topic_q and not text_q:
        raise ValueError("topic_query or text_query must be provided")

    source_filter = {s.lower() for s in (sources or [])}
    start = _parse_date_value(start_date)
    end = _parse_date_value(end_date)

    results: list[dict[str, Any]] = []

    for row in iter_export_rows(input_file):
        source = str(row.get("source") or "").lower()
        if source_filter and source not in source_filter:
            continue

        record_date = _row_record_date(row)
        if start is not None and (record_date is None or record_date < start):
            continue
        if end is not None and (record_date is None or record_date > end):
            continue

        haystack = _row_search_text(row, include_raw_payload=include_raw_payload)

        if topic_q and topic_q not in haystack:
            continue
        if text_q and text_q not in haystack:
            continue

        results.append(row)
        if limit is not None and len(results) >= limit:
            break

    return results


def _top_terms(rows: Iterable[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    tokens: Counter[str] = Counter()

    for row in rows:
        text = " ".join(
            part
            for part in (
                str(row.get("title") or ""),
                str(row.get("abstract") or ""),
                str(row.get("topic") or ""),
            )
            if part
        ).lower()

        for token in re.findall(r"[a-z][a-z0-9_-]{2,}", text):
            if token in _STOPWORDS:
                continue
            tokens[token] += 1

    most_common = tokens.most_common(top_n)
    return [{"term": term, "count": count} for term, count in most_common]


def analyze_topic_trends(
    input_file: str | Path,
    *,
    topic_query: str,
    text_query: str | None = None,
    sources: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    include_raw_payload: bool = True,
    window_days: int = 7,
    lookback_days: int = 90,
    reference_date: date | None = None,
    top_terms_n: int = 12,
) -> dict[str, Any]:
    """Analyze trend velocity for a specific topic/text query over export data."""
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")

    ref_date = reference_date or datetime.now(timezone.utc).date()
    lookback_start = ref_date - timedelta(days=lookback_days - 1)

    # ⚡ Bolt Optimization: Push date bounds down into search_industry_export.
    # This avoids expensive full-text extraction and substring matching
    # on records that are outside the requested lookback window.
    eff_start = lookback_start
    if start_date:
        p_start = _parse_date_value(start_date)
        if p_start and p_start > eff_start:
            eff_start = p_start

    eff_end = ref_date
    if end_date:
        p_end = _parse_date_value(end_date)
        if p_end and p_end < eff_end:
            eff_end = p_end

    matched = search_industry_export(
        input_file,
        topic_query=topic_query,
        text_query=text_query,
        sources=sources,
        start_date=eff_start.isoformat(),
        end_date=eff_end.isoformat(),
        include_raw_payload=include_raw_payload,
        limit=None,
    )

    counts: Counter[date] = Counter()
    recent_rows: list[dict[str, Any]] = []

    for row in matched:
        row_date = _row_record_date(row)
        if row_date is None:
            continue
        if row_date < lookback_start or row_date > ref_date:
            continue

        counts[row_date] += 1
        recent_rows.append(row)

    daily_counts: list[dict[str, Any]] = []
    cursor = lookback_start
    while cursor <= ref_date:
        daily_counts.append(
            {"date": cursor.isoformat(), "count": counts.get(cursor, 0)}
        )
        cursor += timedelta(days=1)

    recent_start = ref_date - timedelta(days=window_days - 1)
    previous_end = recent_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=window_days - 1)

    recent_count = sum(
        c for day, c in counts.items() if recent_start <= day <= ref_date
    )
    previous_count = sum(
        c for day, c in counts.items() if previous_start <= day <= previous_end
    )

    if previous_count == 0:
        growth_rate = None
        trend_status = "emerging" if recent_count > 0 else "flat"
    else:
        growth_rate = ((recent_count - previous_count) / previous_count) * 100.0
        if growth_rate > 15:
            trend_status = "up"
        elif growth_rate < -15:
            trend_status = "down"
        else:
            trend_status = "flat"

    return {
        "topic_query": topic_query,
        "text_query": text_query,
        "matched_records": len(recent_rows),
        "lookback_days": lookback_days,
        "window_days": window_days,
        "reference_date": ref_date.isoformat(),
        "recent_window": {
            "start": recent_start.isoformat(),
            "end": ref_date.isoformat(),
            "count": recent_count,
        },
        "previous_window": {
            "start": previous_start.isoformat(),
            "end": previous_end.isoformat(),
            "count": previous_count,
        },
        "growth_rate_pct": growth_rate,
        "trend_status": trend_status,
        "daily_counts": daily_counts,
        "top_terms": _top_terms(recent_rows, top_n=top_terms_n),
    }
