"""massive-data-ingestion public API."""

from data_ingestion._version import __version__
from data_ingestion.analysis import (
    analyze_topic_trends,
    iter_export_rows,
    search_industry_export,
)
from data_ingestion.config import (
    CrossRefConfig,
    CsvSinkConfig,
    FederalRegisterConfig,
    FetcherSpec,
    FullTextResolutionConfig,
    FullTextSinkConfig,
    HackerNewsConfig,
    HttpClientConfig,
    JsonlSinkConfig,
    NewsApiConfig,
    OpenAlexConfig,
    ParquetSinkConfig,
    PipelineConfig,
    WebsiteConfig,
    WebsiteHtmlConfig,
)
from data_ingestion.factories import build_fetcher, build_fetchers
from data_ingestion.models import (
    FullTextDocument,
    NormalizedRecord,
    PipelineSummary,
    RecordType,
    SourceRunStats,
)
from data_ingestion.pipeline import (
    DataDumperPipeline,
    run_to_jsonl,
    run_to_jsonl_with_full_text,
    stream_records,
    stream_transformed_records,
)
from data_ingestion.registry import list_fetchers
from data_ingestion.transforms import TransformationEngine, TransformationSpec

__all__ = [
    "CrossRefConfig",
    "CsvSinkConfig",
    "DataDumperPipeline",
    "FederalRegisterConfig",
    "FetcherSpec",
    "FullTextDocument",
    "FullTextResolutionConfig",
    "FullTextSinkConfig",
    "HackerNewsConfig",
    "HttpClientConfig",
    "JsonlSinkConfig",
    "NewsApiConfig",
    "NormalizedRecord",
    "OpenAlexConfig",
    "ParquetSinkConfig",
    "PipelineConfig",
    "PipelineSummary",
    "RecordType",
    "SourceRunStats",
    "TransformationEngine",
    "TransformationSpec",
    "WebsiteConfig",
    "WebsiteHtmlConfig",
    "__version__",
    "analyze_topic_trends",
    "build_fetcher",
    "build_fetchers",
    "iter_export_rows",
    "list_fetchers",
    "run_to_jsonl",
    "run_to_jsonl_with_full_text",
    "search_industry_export",
    "stream_records",
    "stream_transformed_records",
]
