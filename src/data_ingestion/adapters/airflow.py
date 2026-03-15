from __future__ import annotations

from typing import TYPE_CHECKING, Any

from data_ingestion.pipeline import stream_records

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from data_ingestion.models import NormalizedRecord


def airflow_ingestion_task(
    fetcher_specs: Sequence[dict[str, Any]],
    *,
    raw: bool = True,
    start_date: str | None = None,
    end_date: str | None = None,
    record_handler: Callable[[str, dict[str, Any] | NormalizedRecord], None]
    | None = None,
) -> dict[str, Any]:
    """Lightweight Airflow-friendly streaming task.

    This adapter intentionally does not persist outputs or apply transforms.
    Users should pass `record_handler` to own filtering, transformation, and writes.
    """
    total_records = 0
    by_source: dict[str, int] = {}

    for source, record in stream_records(
        list(fetcher_specs),
        raw=raw,
        start_date=start_date,
        end_date=end_date,
    ):
        if record_handler is not None:
            record_handler(source, record)

        total_records += 1
        by_source[source] = by_source.get(source, 0) + 1

    return {
        "total_records": total_records,
        "by_source": by_source,
        "raw_mode": raw,
    }
