from __future__ import annotations

from typing import Any

import pytest

from data_ingestion.config import HttpClientConfig
from data_ingestion.exceptions import QuotaExceededError
from data_ingestion.http import (
    RETRIABLE_STATUS_CODES,
    SmartSession,
    _ManagedResponse,
    build_retry_session,
)


class _FakeLimiter:
    def __init__(self) -> None:
        self.acquire_calls = 0
        self.release_calls = 0
        self.cooldowns: list[float] = []

    def acquire(self) -> None:
        self.acquire_calls += 1

    def release(self) -> None:
        self.release_calls += 1

    def apply_cooldown(self, seconds: float) -> None:
        self.cooldowns.append(seconds)


class _FakeRegistry:
    def __init__(self, limiter: _FakeLimiter) -> None:
        self.limiter = limiter

    def get_limiter(self, url: str, policy: Any) -> _FakeLimiter:
        return self.limiter


class _FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self.headers: dict[str, str] = {}
        self.closed = False

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        if not self._responses:
            raise RuntimeError("no responses left")
        return self._responses.pop(0)

    def close(self) -> None:
        self.closed = True


class _FailingSession(_FakeSession):
    def __init__(self) -> None:
        super().__init__(responses=[])

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        raise RuntimeError("network boom")


def test_managed_response_releases_once() -> None:
    release_counter = {"count": 0}

    def release() -> None:
        release_counter["count"] += 1

    resp = _ManagedResponse(_FakeResponse(), release)
    resp.close()
    resp.close()

    assert release_counter["count"] == 1


def test_smart_session_non_stream_success(monkeypatch) -> None:
    limiter = _FakeLimiter()
    monkeypatch.setattr(
        "data_ingestion.http.GLOBAL_RATE_LIMITER_REGISTRY",
        _FakeRegistry(limiter),
    )

    cfg = HttpClientConfig(max_retries=0)
    session = SmartSession(_FakeSession([_FakeResponse(200)]), cfg)

    resp = session.get("https://example.com")
    assert isinstance(resp, _FakeResponse)
    assert limiter.acquire_calls == 1
    assert limiter.release_calls == 1


def test_smart_session_stream_returns_managed_response(monkeypatch) -> None:
    limiter = _FakeLimiter()
    monkeypatch.setattr(
        "data_ingestion.http.GLOBAL_RATE_LIMITER_REGISTRY",
        _FakeRegistry(limiter),
    )

    cfg = HttpClientConfig(max_retries=0)
    session = SmartSession(_FakeSession([_FakeResponse(200)]), cfg)

    resp = session.get("https://example.com", stream=True)
    assert isinstance(resp, _ManagedResponse)
    assert limiter.release_calls == 0

    resp.close()
    assert limiter.release_calls == 1


def test_smart_session_retries_and_applies_cooldown(monkeypatch) -> None:
    limiter = _FakeLimiter()
    monkeypatch.setattr(
        "data_ingestion.http.GLOBAL_RATE_LIMITER_REGISTRY",
        _FakeRegistry(limiter),
    )
    monkeypatch.setattr("data_ingestion.http.parse_retry_after", lambda _: 1.5)

    cfg = HttpClientConfig(max_retries=1)
    first = _FakeResponse(
        next(iter(RETRIABLE_STATUS_CODES)),
        headers={"Retry-After": "2"},
    )
    second = _FakeResponse(200)
    session = SmartSession(_FakeSession([first, second]), cfg)

    resp = session.get("https://example.com")
    assert isinstance(resp, _FakeResponse)
    assert limiter.acquire_calls == 2
    assert limiter.release_calls == 2
    assert limiter.cooldowns == [1.5]
    assert first.closed is True


def test_smart_session_releases_limiter_on_get_exception(monkeypatch) -> None:
    limiter = _FakeLimiter()
    monkeypatch.setattr(
        "data_ingestion.http.GLOBAL_RATE_LIMITER_REGISTRY",
        _FakeRegistry(limiter),
    )

    cfg = HttpClientConfig(max_retries=0)
    session = SmartSession(_FailingSession(), cfg)

    with pytest.raises(RuntimeError, match="network boom"):
        session.get("https://example.com")

    assert limiter.acquire_calls == 1
    assert limiter.release_calls == 1


def test_smart_session_blocks_request_after_budget(monkeypatch) -> None:
    limiter = _FakeLimiter()
    monkeypatch.setattr(
        "data_ingestion.http.GLOBAL_RATE_LIMITER_REGISTRY",
        _FakeRegistry(limiter),
    )

    cfg = HttpClientConfig(max_retries=0, max_requests_per_session=1)
    session = SmartSession(_FakeSession([_FakeResponse(200)]), cfg)

    assert isinstance(session.get("https://example.com"), _FakeResponse)
    with pytest.raises(QuotaExceededError, match="quota exhausted"):
        session.get("https://example.com")

    assert limiter.acquire_calls == 1
    assert limiter.release_calls == 1


def test_smart_session_retries_consume_request_budget(monkeypatch) -> None:
    limiter = _FakeLimiter()
    monkeypatch.setattr(
        "data_ingestion.http.GLOBAL_RATE_LIMITER_REGISTRY",
        _FakeRegistry(limiter),
    )
    monkeypatch.setattr("data_ingestion.http.parse_retry_after", lambda _: 0.0)

    cfg = HttpClientConfig(max_retries=1, max_requests_per_session=1)
    first = _FakeResponse(next(iter(RETRIABLE_STATUS_CODES)))
    session = SmartSession(_FakeSession([first, _FakeResponse(200)]), cfg)

    with pytest.raises(QuotaExceededError, match="quota exhausted"):
        session.get("https://example.com")

    assert limiter.acquire_calls == 1
    assert limiter.release_calls == 1
    assert first.closed is True


def test_build_retry_session_sets_headers() -> None:
    cfg = HttpClientConfig(user_agent="mdi-test/1.0", email="a@example.com")
    session = build_retry_session(cfg)

    assert session.headers["User-Agent"] == "mdi-test/1.0"
    assert session.headers["From"] == "a@example.com"
    session.close()
