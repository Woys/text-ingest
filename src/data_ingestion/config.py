from __future__ import annotations

import os
from datetime import date  # noqa: TC003
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class HttpClientConfig(BaseModel):
    timeout_seconds: int = Field(default=30, ge=1, le=300)
    max_retries: int = Field(default=3, ge=0, le=10)
    backoff_factor: float = Field(default=0.5, ge=0.0, le=60.0)
    user_agent: str = Field(default="massive-data-ingestion/0.2.0", min_length=1)
    email: str | None = None

    requests_per_second: float = Field(default=1.0, gt=0.0, le=100.0)
    burst_size: int = Field(default=1, ge=1, le=100)
    max_concurrent_requests: int = Field(default=1, ge=1, le=100)
    respect_retry_after: bool = True
    max_retry_after_seconds: int = Field(default=120, ge=1, le=3600)
    jitter_seconds: float = Field(default=0.5, ge=0.0, le=10.0)


class BaseSourceConfig(BaseModel):
    query: str | None = None
    max_pages: int = Field(default=3, ge=1, le=100)
    start_date: date | None = None
    end_date: date | None = None

    search_mode: Literal["exact", "broad", "fuzzy_local", "date_only"] = "broad"
    date_mode: Literal["publication", "update"] = "publication"

    fuzzy_terms: list[str] = Field(default_factory=list)
    fuzzy_threshold: int = Field(default=85, ge=0, le=100)

    # Optional content-level filtering applied after normalization.
    topic_include: list[str] = Field(default_factory=list)
    topic_exclude: list[str] = Field(default_factory=list)

    @field_validator("query")
    @classmethod
    def _strip_and_validate_query(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("query must not be blank")
        return cleaned

    @field_validator("topic_include", "topic_exclude", mode="before")
    @classmethod
    def _coerce_topic_terms(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    @field_validator("topic_include", "topic_exclude")
    @classmethod
    def _normalize_topic_terms(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for term in value:
            normalized = term.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            cleaned.append(normalized)
        return cleaned

    @model_validator(mode="after")
    def validate_mode(self) -> BaseSourceConfig:
        if (
            self.search_mode in {"exact", "broad"}
            and not self.query
            and not (self.start_date and self.end_date)
        ):
            raise ValueError(
                "Provide query, or provide a date range, or use "
                "fuzzy_local/date_only mode."
            )

        if self.search_mode == "fuzzy_local" and not self.fuzzy_terms:
            raise ValueError("fuzzy_local mode requires fuzzy_terms")

        if (
            self.start_date is not None
            and self.end_date is not None
            and self.start_date > self.end_date
        ):
            raise ValueError("start_date must be <= end_date")

        overlap = set(self.topic_include) & set(self.topic_exclude)
        if overlap:
            overlap_str = ", ".join(sorted(overlap))
            raise ValueError(
                f"topic_include/topic_exclude overlap is not allowed: {overlap_str}"
            )

        return self


class OpenAlexConfig(BaseSourceConfig):
    per_page: int = Field(default=50, ge=1, le=200)
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)


class CrossRefConfig(BaseSourceConfig):
    rows: int = Field(default=50, ge=1, le=1000)
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)


class NewsApiConfig(BaseSourceConfig):
    api_key: str = Field(default="", validate_default=True)
    page_size: int = Field(default=20, ge=1, le=100)
    language: str = Field(default="en", min_length=2, max_length=5)
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)

    @field_validator("api_key", mode="before")
    @classmethod
    def _resolve_api_key(cls, value: str) -> str:
        if not value:
            value = os.getenv("NEWSAPI_KEY", "")
        if not value:
            raise ValueError(
                "NewsAPI key is required. Pass it as 'api_key' in the config "
                "or set the NEWSAPI_KEY environment variable."
            )
        return value


class HackerNewsConfig(BaseSourceConfig):
    hits_per_page: int = Field(default=100, ge=1, le=1000)
    hn_item_type: Literal["story", "comment", "all"] = "story"
    use_date_sort: bool = True
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)


class FederalRegisterConfig(BaseSourceConfig):
    per_page: int = Field(default=50, ge=1, le=1000)
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)


class FetcherSpec(BaseModel):
    source: Literal["openalex", "crossref", "newsapi", "hackernews", "federalregister"]
    config: dict[str, Any]


class JsonlSinkConfig(BaseModel):
    output_file: str = Field(..., min_length=1)
    append: bool = True
    encoding: str = "utf-8"


class CsvSinkConfig(BaseModel):
    output_file: str = Field(..., min_length=1)
    append: bool = True


class ParquetSinkConfig(BaseModel):
    output_file: str = Field(..., min_length=1)
    batch_size: int = Field(default=1000, ge=1)
    compression: str = Field(default="snappy")


class FullTextSinkConfig(BaseModel):
    output_file: str = Field(..., min_length=1)
    append: bool = True
    encoding: str = "utf-8"


class FullTextResolutionConfig(BaseModel):
    max_chars: int = Field(default=200_000, ge=1)
    max_download_bytes: int = Field(default=25_000_000, ge=1)
    spool_max_memory_bytes: int = Field(default=2_000_000, ge=1)
    download_chunk_size: int = Field(default=64 * 1024, ge=1024)
    max_workers: int = Field(default=8, ge=1, le=64)
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)


class RuntimeOptimizationConfig(BaseModel):
    enrich_full_text: bool = False
    write_raw_payload: bool = False
    full_text_batch_size: int = Field(default=10, ge=1, le=1000)
    sink_write_batch_size: int = Field(default=500, ge=1, le=100_000)
    partition_by_source: bool = False
    partition_by_date: bool = False


class PipelineConfig(BaseModel):
    fail_fast: bool = True
    runtime: RuntimeOptimizationConfig = Field(
        default_factory=RuntimeOptimizationConfig
    )
