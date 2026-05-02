"""Retry-enabled and rate-limit-aware HTTP session factory."""

from __future__ import annotations

import threading
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from data_ingestion.exceptions import QuotaExceededError
from data_ingestion.rate_limit import (
    GLOBAL_RATE_LIMITER_REGISTRY,
    RateLimitPolicy,
    compute_backoff_seconds,
    parse_retry_after,
)

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from data_ingestion.config import HttpClientConfig


RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class _ManagedResponse:
    def __init__(self, response: requests.Response, release_callback: Any) -> None:
        self._response = response
        self._release_callback = release_callback
        self._released = False

    def _release(self) -> None:
        if not self._released:
            self._release_callback()
            self._released = True

    def close(self) -> None:
        try:
            self._response.close()
        finally:
            self._release()

    def __enter__(self) -> _ManagedResponse:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._response, name)


class SmartSession:
    def __init__(self, session: requests.Session, config: HttpClientConfig) -> None:
        self._session = session
        self._config = config
        self._policy = RateLimitPolicy.from_http_config(config)
        self._request_count = 0
        self._request_count_lock = threading.Lock()

    @property
    def headers(self) -> MutableMapping[str, str | bytes]:
        return self._session.headers

    def close(self) -> None:
        self._session.close()

    def _reserve_request_budget(self, url: str) -> None:
        max_requests = self._config.max_requests_per_session
        if max_requests is None:
            return

        with self._request_count_lock:
            if self._request_count >= max_requests:
                raise QuotaExceededError(
                    "Configured HTTP request quota exhausted "
                    f"for this session: max_requests_per_session={max_requests}, "
                    f"url={url}"
                )
            self._request_count += 1

    def get(self, url: str, **kwargs: Any) -> requests.Response | _ManagedResponse:
        stream = bool(kwargs.get("stream", False))
        attempts = max(1, self._config.max_retries + 1)
        limiter = GLOBAL_RATE_LIMITER_REGISTRY.get_limiter(url, self._policy)

        for attempt in range(1, attempts + 1):
            self._reserve_request_budget(url)
            limiter.acquire()

            try:
                response = self._session.get(url, **kwargs)
            except Exception:
                limiter.release()
                raise

            should_retry = (
                response.status_code in RETRIABLE_STATUS_CODES and attempt < attempts
            )

            if should_retry:
                retry_after_seconds = None
                if self._policy.respect_retry_after:
                    retry_after_seconds = parse_retry_after(
                        response.headers.get("Retry-After")
                    )

                cooldown_seconds = (
                    retry_after_seconds
                    if retry_after_seconds is not None
                    else compute_backoff_seconds(
                        attempt,
                        backoff_factor=self._config.backoff_factor,
                        jitter_seconds=self._policy.jitter_seconds,
                    )
                )

                response.close()
                limiter.release()
                limiter.apply_cooldown(cooldown_seconds)
                continue

            if stream:
                return _ManagedResponse(response, limiter.release)

            limiter.release()
            return response

        raise RuntimeError("Unreachable code path in SmartSession.get")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)


def build_retry_session(config: HttpClientConfig) -> SmartSession:
    retry = Retry(
        total=config.max_retries,
        read=config.max_retries,
        connect=config.max_retries,
        backoff_factor=config.backoff_factor,
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
        status=0,
        respect_retry_after_header=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=max(10, config.max_concurrent_requests * 2),
        pool_maxsize=max(20, config.max_concurrent_requests * 4),
    )

    session = requests.Session()
    session.headers.update({"User-Agent": config.user_agent})
    if config.email:
        session.headers.update({"From": config.email})

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return SmartSession(session, config)
