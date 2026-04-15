"""Tests for utils.rate_limiter.AsyncRateLimiter."""

import asyncio

from utils.rate_limiter import AsyncRateLimiter


def test_acquire_returns_after_wait() -> None:
    async def _run():
        limiter = AsyncRateLimiter(requests_per_minute=600.0)
        waited = await limiter.acquire()
        return waited, limiter

    waited, limiter = asyncio.run(_run())
    assert waited >= 0.0
    assert limiter.total_requests == 1
