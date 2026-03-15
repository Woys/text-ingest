"""Turn declarative FetcherSpec dicts into ready-to-use fetcher instances."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from data_ingestion.config import FetcherSpec
from data_ingestion.fetchers import crossref as _crossref  # noqa: F401
from data_ingestion.fetchers import federal_register as _federal_register  # noqa: F401
from data_ingestion.fetchers import hackernews as _hackernews  # noqa: F401
from data_ingestion.fetchers import newsapi as _newsapi  # noqa: F401
from data_ingestion.fetchers import openalex as _openalex  # noqa: F401
from data_ingestion.logging_utils import get_logger
from data_ingestion.registry import get_fetcher_class

if TYPE_CHECKING:
    from collections.abc import Iterable

    from data_ingestion.fetchers.base import BaseFetcher

logger = get_logger(__name__)


def build_fetcher(spec: FetcherSpec | dict[str, Any]) -> BaseFetcher:
    """Validate *spec*, resolve the fetcher class, and return an instance."""
    validated = (
        spec if isinstance(spec, FetcherSpec) else FetcherSpec.model_validate(spec)
    )

    logger.debug("Building fetcher for source=%s", validated.source)
    fetcher_cls = get_fetcher_class(validated.source)
    config = fetcher_cls.config_model.model_validate(validated.config)
    fetcher = fetcher_cls(config)

    logger.info(
        "Built fetcher source=%s class=%s", validated.source, fetcher_cls.__name__
    )
    return fetcher


def build_fetchers(specs: Iterable[FetcherSpec | dict[str, Any]]) -> list[BaseFetcher]:
    """Build a list of fetchers from an iterable of specs."""
    fetchers = [build_fetcher(spec) for spec in specs]
    logger.info("Built %d fetcher(s)", len(fetchers))
    return fetchers
