"""Per-host rate limiting utilities."""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from data_ingestion.config import HttpClientConfig


@dataclass(frozen=True)
class RateLimitPolicy:
    requests_per_second: float
    burst_size: int
    max_concurrent_requests: int
    respect_retry_after: bool
    max_retry_after_seconds: int
    jitter_seconds: float

    @classmethod
    def from_http_config(cls, config: HttpClientConfig) -> RateLimitPolicy:
        return cls(
            requests_per_second=config.requests_per_second,
            burst_size=config.burst_size,
            max_concurrent_requests=config.max_concurrent_requests,
            respect_retry_after=config.respect_retry_after,
            max_retry_after_seconds=config.max_retry_after_seconds,
            jitter_seconds=config.jitter_seconds,
        )


def parse_retry_after(header_value: str | None) -> float | None:
    if not header_value:
        return None

    stripped = header_value.strip()
    if not stripped:
        return None

    try:
        seconds = int(stripped)
        return max(0.0, float(seconds))
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, IndexError):
        return None

    if dt is None or not isinstance(dt, datetime):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    delay_seconds = float(
        (dt.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()
    )
    return max(0.0, delay_seconds)


def compute_backoff_seconds(
    attempt_number: int,
    *,
    backoff_factor: float,
    jitter_seconds: float,
) -> float:
    wait: float = backoff_factor * (2 ** max(0, attempt_number - 1))
    if jitter_seconds > 0:
        wait += float(random.uniform(0.0, jitter_seconds))
    return float(max(0.0, wait))


class _TokenBucket:
    def __init__(self, rate: float, capacity: int, jitter_seconds: float) -> None:
        self._rate = rate
        self._capacity = float(capacity)
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._jitter_seconds = jitter_seconds
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._last_refill = now

                self._tokens = min(
                    self._capacity,
                    self._tokens + (elapsed * self._rate),
                )

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                missing = 1.0 - self._tokens
                wait_seconds = missing / self._rate

            if self._jitter_seconds > 0:
                wait_seconds += random.uniform(0.0, self._jitter_seconds)

            time.sleep(wait_seconds)


class PerHostRateLimiter:
    def __init__(self, policy: RateLimitPolicy) -> None:
        self.policy = policy
        self._bucket = _TokenBucket(
            rate=policy.requests_per_second,
            capacity=policy.burst_size,
            jitter_seconds=policy.jitter_seconds,
        )
        self._semaphore = threading.BoundedSemaphore(policy.max_concurrent_requests)
        self._cooldown_until = 0.0
        self._cooldown_lock = threading.Lock()

    def _wait_for_cooldown(self) -> None:
        while True:
            with self._cooldown_lock:
                remaining = self._cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(remaining)

    def acquire(self) -> None:
        while True:
            self._wait_for_cooldown()
            self._bucket.acquire()
            self._wait_for_cooldown()
            self._semaphore.acquire()
            self._wait_for_cooldown()
            return

    def release(self) -> None:
        self._semaphore.release()

    def apply_cooldown(self, seconds: float) -> None:
        if seconds <= 0:
            return

        bounded = min(seconds, float(self.policy.max_retry_after_seconds))
        new_until = time.monotonic() + bounded

        with self._cooldown_lock:
            self._cooldown_until = max(self._cooldown_until, new_until)


class HostRateLimiterRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._limiters: dict[str, PerHostRateLimiter] = {}

    def get_limiter(self, url: str, policy: RateLimitPolicy) -> PerHostRateLimiter:
        host = urlparse(url).netloc.lower() or "default"

        with self._lock:
            limiter = self._limiters.get(host)
            if limiter is None:
                limiter = PerHostRateLimiter(policy)
                self._limiters[host] = limiter
            return limiter


GLOBAL_RATE_LIMITER_REGISTRY = HostRateLimiterRegistry()
