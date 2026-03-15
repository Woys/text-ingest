import pytest
from pydantic import ValidationError

from data_ingestion.config import (
    CrossRefConfig,
    FetcherSpec,
    JsonlSinkConfig,
    NewsApiConfig,
    OpenAlexConfig,
    PipelineConfig,
)


def test_openalex_config_strips_query_whitespace() -> None:
    config = OpenAlexConfig(query=" cancer prevention ")
    assert config.query == "cancer prevention"
    assert config.max_pages == 3
    assert config.per_page == 50


def test_crossref_config_rejects_blank_query() -> None:
    with pytest.raises(ValidationError):
        CrossRefConfig(query="   ")


def test_fetcher_spec_accepts_all_sources() -> None:
    for source in ("openalex", "crossref", "newsapi"):
        spec = FetcherSpec(source=source, config={"query": "test", "api_key": "k"})  # type: ignore[arg-type]
        assert spec.source == source


def test_fetcher_spec_rejects_unknown_source() -> None:
    with pytest.raises(ValidationError):
        FetcherSpec(source="unknown", config={})  # type: ignore[arg-type]


def test_jsonl_sink_config_defaults() -> None:
    config = JsonlSinkConfig(output_file="data/out.jsonl", append=False)
    assert config.output_file == "data/out.jsonl"
    assert config.append is False
    assert config.encoding == "utf-8"


def test_pipeline_config_defaults() -> None:
    assert PipelineConfig().fail_fast is True


def test_newsapi_config_explicit_key() -> None:
    config = NewsApiConfig(query="test", api_key="abc123")
    assert config.api_key == "abc123"
    assert config.page_size == 20
    assert config.language == "en"


def test_newsapi_config_reads_key_from_env(monkeypatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "env_key_xyz")
    config = NewsApiConfig(query="test")
    assert config.api_key == "env_key_xyz"


def test_newsapi_config_raises_when_no_key(monkeypatch) -> None:
    monkeypatch.delenv("NEWSAPI_KEY", raising=False)
    with pytest.raises(ValidationError, match="key is required"):
        NewsApiConfig(query="test", api_key="")


def test_topic_filters_are_normalized() -> None:
    config = OpenAlexConfig(
        query="test",
        topic_include=["  AI  ", "ai", "Cancer"],
        topic_exclude="  politics  ",
    )
    assert config.topic_include == ["ai", "cancer"]
    assert config.topic_exclude == ["politics"]


def test_topic_filters_reject_overlap() -> None:
    with pytest.raises(ValidationError, match="overlap"):
        OpenAlexConfig(
            query="test",
            topic_include=["ai", "health"],
            topic_exclude=["health"],
        )
