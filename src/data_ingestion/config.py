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
    max_requests_per_session: int | None = Field(default=1000, ge=1)


def _http_config(
    *,
    requests_per_second: float = 1.0,
    max_requests_per_session: int | None = 1000,
) -> HttpClientConfig:
    return HttpClientConfig(
        requests_per_second=requests_per_second,
        max_requests_per_session=max_requests_per_session,
    )


class BaseSourceConfig(BaseModel):
    query: str | None = None
    max_pages: int | None = Field(default=3, ge=0)
    start_date: date | None = None
    end_date: date | None = None
    languages: list[str] = Field(default_factory=list)

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

    @field_validator("languages", mode="before")
    @classmethod
    def _coerce_languages(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    @field_validator("languages")
    @classmethod
    def _normalize_languages(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for language in value:
            normalized = language.strip().lower()
            if not normalized:
                continue
            normalized = normalized.replace("_", "-")
            if normalized in seen:
                continue
            seen.add(normalized)
            cleaned.append(normalized)
        return cleaned

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
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=5000)
    )


class CrossRefConfig(BaseSourceConfig):
    rows: int = Field(default=50, ge=1, le=1000)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=5000)
    )


class NewsApiConfig(BaseSourceConfig):
    api_key: str = Field(default="", validate_default=True)
    page_size: int = Field(default=20, ge=1, le=100)
    language: str = Field(default="en", min_length=2, max_length=5)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=90)
    )

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
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=1000)
    )


class FederalRegisterConfig(BaseSourceConfig):
    per_page: int = Field(default=50, ge=1, le=1000)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=1000)
    )


class EdgarConfig(BaseSourceConfig):
    per_page: int = Field(default=50, ge=1, le=1000)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=5000)
    )


class WikipediaConfig(BaseSourceConfig):
    wiki_language: str = Field(default="en", min_length=2, max_length=10)
    page_size: int = Field(default=20, ge=1, le=50)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=1000)
    )


class RedditConfig(BaseSourceConfig):
    subreddit: str | None = None
    sort: Literal["new", "hot", "top", "relevance"] = "new"
    page_size: int = Field(default=25, ge=1, le=100)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=600)
    )


class GitHubConfig(BaseSourceConfig):
    per_page: int = Field(default=25, ge=1, le=100)
    sort: Literal["updated", "stars", "forks"] = "updated"
    github_token: str | None = None
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=60)
    )

    @field_validator("github_token", mode="before")
    @classmethod
    def _resolve_github_token(cls, value: str | None) -> str | None:
        if value is None:
            value = os.getenv("GITHUB_TOKEN")
        return value


class StackExchangeConfig(BaseSourceConfig):
    site: str = Field(default="stackoverflow", min_length=2)
    page_size: int = Field(default=25, ge=1, le=100)
    sort: Literal["activity", "creation", "votes"] = "activity"
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=250)
    )


class OpenLibraryConfig(BaseSourceConfig):
    page_size: int = Field(default=25, ge=1, le=100)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=1000)
    )


class GoogleNewsConfig(BaseSourceConfig):
    hl: str = Field(default="en-US", min_length=2, max_length=20)
    gl: str = Field(default="US", min_length=2, max_length=10)
    ceid: str = Field(default="US:en", min_length=2, max_length=20)
    page_size: int = Field(default=50, ge=1, le=1000)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(
            requests_per_second=0.2,
            max_requests_per_session=200,
        )
    )


class GuardianConfig(BaseSourceConfig):
    page_size: int = Field(default=25, ge=1, le=200)
    api_key: str = Field(default="test", min_length=1)
    http: HttpClientConfig = Field(
        default_factory=lambda: _http_config(max_requests_per_session=500)
    )

    @field_validator("api_key", mode="before")
    @classmethod
    def _resolve_guardian_api_key(cls, value: str | None) -> str:
        if value is None:
            value = os.getenv("GUARDIAN_API_KEY", "test")
        cleaned = value.strip()
        return cleaned or "test"


class WebsiteConfig(BaseSourceConfig):
    feed_url: str | None = None
    site_url: str | None = None
    target_date: date | None = None
    max_items: int = Field(default=100, ge=1, le=10_000)
    search_mode: Literal["exact", "broad", "fuzzy_local", "date_only"] = "date_only"
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)

    @field_validator("feed_url", "site_url")
    @classmethod
    def _normalize_optional_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        return cleaned

    @model_validator(mode="after")
    def _validate_feed_or_site_url(self) -> WebsiteConfig:
        if not self.feed_url and not self.site_url:
            raise ValueError("Provide at least one of 'feed_url' or 'site_url'.")

        if self.target_date is not None:
            if self.start_date is not None and self.start_date != self.target_date:
                raise ValueError("target_date conflicts with start_date.")
            if self.end_date is not None and self.end_date != self.target_date:
                raise ValueError("target_date conflicts with end_date.")
            self.start_date = self.target_date
            self.end_date = self.target_date

        return self


class WebsiteHtmlConfig(BaseSourceConfig):
    site_url: str
    list_page_urls: list[str] = Field(default_factory=list)
    max_items: int = Field(default=100, ge=1, le=10_000)
    max_candidate_links: int = Field(default=400, ge=1, le=50_000)
    include_list_pages_as_items: bool = True
    link_include_patterns: list[str] = Field(default_factory=list)
    link_exclude_patterns: list[str] = Field(default_factory=list)
    search_mode: Literal["exact", "broad", "fuzzy_local", "date_only"] = "date_only"
    http: HttpClientConfig = Field(default_factory=HttpClientConfig)

    @field_validator("site_url")
    @classmethod
    def _normalize_site_url(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("site_url must not be blank")
        return cleaned

    @field_validator("list_page_urls", mode="before")
    @classmethod
    def _coerce_list_page_urls(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    @field_validator(
        "list_page_urls",
        "link_include_patterns",
        "link_exclude_patterns",
        mode="before",
    )
    @classmethod
    def _coerce_str_list(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    @field_validator("list_page_urls", "link_include_patterns", "link_exclude_patterns")
    @classmethod
    def _normalize_str_list(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in value:
            normalized = item.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            cleaned.append(normalized)
        return cleaned


class FetcherSpec(BaseModel):
    source: Literal[
        "openalex",
        "crossref",
        "newsapi",
        "hackernews",
        "federalregister",
        "wikipedia",
        "reddit",
        "github",
        "stackexchange",
        "openlibrary",
        "googlenews",
        "guardian",
        "website",
        "website_html",
        "edgar",
    ]
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
