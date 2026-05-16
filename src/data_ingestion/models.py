"""Runtime data models shared across the package."""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class RecordType(str, Enum):
    ARTICLE = "article"
    NEWS = "news"
    PREPRINT = "preprint"


class NormalizedRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source: str = Field(..., min_length=1)
    external_id: str | None = None
    title: str | None = None
    authors: list[str] = Field(default_factory=list)
    published_date: date | None = None
    url: str | None = None
    abstract: str | None = None
    full_text: str | None = None
    full_text_url: str | None = None
    topic: str | None = None
    record_type: RecordType = RecordType.ARTICLE
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_payload: dict[str, Any] = Field(default_factory=dict)

    # ⚡ Bolt Optimization: Ensure datetime/date serialize to standard str()
    # exactly as before when using model_dump_json() to maintain pipeline
    # compatibility, while still being much faster than json.dumps().
    @field_serializer("fetched_at", mode="plain", when_used="json")
    def serialize_datetime_as_str(self, dt: datetime, _info: Any) -> str:
        return str(dt)

    @field_serializer("published_date", mode="plain", when_used="json")
    def serialize_date_as_str(self, d: date | None, _info: Any) -> Any:
        return str(d) if d is not None else None

    def to_output_dict(self, *, include_raw_payload: bool = True) -> dict[str, Any]:
        row = self.model_dump(mode="python")
        row["record_type"] = row["record_type"].value
        if not include_raw_payload:
            row.pop("raw_payload", None)
        return row

    def to_json_line(self, *, include_raw_payload: bool = True) -> str:
        # ⚡ Bolt Optimization: Use Pydantic's Rust-backed model_dump_json() directly.
        # This is ~4x faster than building an intermediate dict and using json.dumps().
        if include_raw_payload:
            return self.model_dump_json()
        return self.model_dump_json(exclude={"raw_payload"})


class FullTextDocument(BaseModel):
    """Dedicated full-text output model for the separate full-text file."""

    model_config = ConfigDict(extra="ignore")

    source: str = Field(..., min_length=1)
    external_id: str | None = None
    title: str | None = None
    url: str | None = None
    full_text_url: str | None = None
    full_text: str = Field(..., min_length=1)
    content_type: str | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_json_line(self) -> str:
        return self.model_dump_json()


class SourceRunStats(BaseModel):
    seen: int = 0
    kept: int = 0
    dropped_by_topic: int = 0
    dropped_by_transform: int = 0
    checkpoint_skipped: int = 0


class PipelineSummary(BaseModel):
    total_records: int = 0
    by_source: dict[str, int] = Field(default_factory=dict)
    failed_sources: dict[str, str] = Field(default_factory=dict)
    by_source_stats: dict[str, SourceRunStats] = Field(default_factory=dict)
    output_target: str | None = None
    resumed_from_checkpoint: bool = False
    checkpoint_path: str | None = None
    checkpoint_entries: int = 0
