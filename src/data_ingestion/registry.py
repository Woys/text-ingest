"""Simple string-to-class registry for fetchers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_ingestion.exceptions import ConfigurationError
from data_ingestion.fetchers.base import BaseFetcher

if TYPE_CHECKING:
    from collections.abc import Callable

FetcherType = type[BaseFetcher]
_FETCHER_REGISTRY: dict[str, FetcherType] = {}


def register_fetcher(name: str) -> Callable[[FetcherType], FetcherType]:
    """Class decorator that registers *fetcher_cls* under *name*."""

    def decorator(fetcher_cls: FetcherType) -> FetcherType:
        _FETCHER_REGISTRY[name] = fetcher_cls
        return fetcher_cls

    return decorator


def get_fetcher_class(name: str) -> FetcherType:
    if name not in _FETCHER_REGISTRY:
        available = ", ".join(sorted(_FETCHER_REGISTRY))
        raise ConfigurationError(
            f"Unsupported source: '{name}'.  Available: {available or '(none)'}"
        )
    return _FETCHER_REGISTRY[name]


def list_fetchers() -> list[str]:
    return sorted(_FETCHER_REGISTRY.keys())
