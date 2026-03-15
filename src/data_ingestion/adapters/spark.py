from __future__ import annotations

from typing import TYPE_CHECKING, Any

from data_ingestion.pipeline import stream_records

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from data_ingestion.models import NormalizedRecord


def stream_records_for_spark(
    fetcher_specs: Sequence[dict[str, Any]],
    *,
    raw: bool = True,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Iterator[tuple[str, dict[str, Any] | NormalizedRecord]]:
    """Yield source-tagged records for user-managed Spark processing."""
    yield from stream_records(
        list(fetcher_specs),
        raw=raw,
        start_date=start_date,
        end_date=end_date,
    )


def records_to_dataframe(spark: Any, rows: Iterable[dict[str, Any]]) -> Any:
    """Create a DataFrame from user-transformed row dictionaries."""
    return spark.createDataFrame(rows)


def load_jsonl_to_dataframe(spark: Any, path: str) -> Any:
    """Utility for loading user-produced JSONL outputs."""
    return spark.read.json(path)


def load_parquet_to_dataframe(spark: Any, path: str) -> Any:
    """Utility for loading user-produced Parquet outputs."""
    return spark.read.parquet(path)
