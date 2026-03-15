from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

from data_ingestion.rate_limit import (
    HostRateLimiterRegistry,
    PerHostRateLimiter,
    RateLimitPolicy,
    _TokenBucket,
    compute_backoff_seconds,
    parse_retry_after,
)


def test_parse_retry_after_seconds_and_blank_cases() -> None:
    assert parse_retry_after(None) is None
    assert parse_retry_after("   ") is None
    assert parse_retry_after("10") == 10.0


def test_parse_retry_after_http_date_and_invalid_value() -> None:
    dt = datetime.now(timezone.utc) + timedelta(seconds=20)
    header = format_datetime(dt)
    parsed = parse_retry_after(header)
    assert parsed is not None
    assert parsed >= 0
    assert parse_retry_after("not-a-date") is None


def test_compute_backoff_seconds_without_jitter() -> None:
    assert compute_backoff_seconds(1, backoff_factor=0.5, jitter_seconds=0.0) == 0.5
    assert compute_backoff_seconds(2, backoff_factor=0.5, jitter_seconds=0.0) == 1.0


def test_compute_backoff_seconds_with_jitter(monkeypatch) -> None:
    monkeypatch.setattr("data_ingestion.rate_limit.random.uniform", lambda a, b: 0.2)
    assert compute_backoff_seconds(2, backoff_factor=1.0, jitter_seconds=0.5) == 2.2


def test_token_bucket_acquire_consumes_token_without_wait(monkeypatch) -> None:
    bucket = _TokenBucket(rate=10.0, capacity=2, jitter_seconds=0.0)
    monkeypatch.setattr("data_ingestion.rate_limit.time.sleep", lambda _: None)
    bucket.acquire()
    bucket.acquire()


def test_per_host_limiter_acquire_release_and_cooldown(monkeypatch) -> None:
    policy = RateLimitPolicy(
        requests_per_second=100.0,
        burst_size=2,
        max_concurrent_requests=1,
        respect_retry_after=True,
        max_retry_after_seconds=3,
        jitter_seconds=0.0,
    )
    limiter = PerHostRateLimiter(policy)

    sleep_calls: list[float] = []
    monkeypatch.setattr(
        "data_ingestion.rate_limit.time.sleep",
        lambda s: sleep_calls.append(s),
    )

    limiter.acquire()
    limiter.release()

    limiter.apply_cooldown(100.0)
    limiter.acquire()
    limiter.release()

    assert all(call >= 0 for call in sleep_calls)


def test_registry_returns_same_limiter_per_host() -> None:
    policy = RateLimitPolicy(
        requests_per_second=1.0,
        burst_size=1,
        max_concurrent_requests=1,
        respect_retry_after=True,
        max_retry_after_seconds=10,
        jitter_seconds=0.0,
    )
    registry = HostRateLimiterRegistry()

    l1 = registry.get_limiter("https://example.com/a", policy)
    l2 = registry.get_limiter("https://example.com/b", policy)
    l3 = registry.get_limiter("https://another.example/path", policy)

    assert l1 is l2
    assert l1 is not l3


def test_rate_limit_policy_from_http_config() -> None:
    from data_ingestion.config import HttpClientConfig

    cfg = HttpClientConfig(
        requests_per_second=2.5,
        burst_size=3,
        max_concurrent_requests=4,
        respect_retry_after=False,
        max_retry_after_seconds=30,
        jitter_seconds=0.1,
    )
    policy = RateLimitPolicy.from_http_config(cfg)

    assert policy.requests_per_second == 2.5
    assert policy.burst_size == 3
    assert policy.max_concurrent_requests == 4
    assert policy.respect_retry_after is False
    assert policy.max_retry_after_seconds == 30
    assert policy.jitter_seconds == 0.1
