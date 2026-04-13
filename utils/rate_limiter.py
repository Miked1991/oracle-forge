"""Async rate limiter for Groq API calls to stay within free tier limits."""

import asyncio
import time
from typing import Any, Dict, Optional

class AsyncRateLimiter:
    """
    Token bucket rate limiter with configurable requests per minute.
    
    Features:
    - Burst handling
    - Queue for pending requests
    - Metrics for monitoring
    
    Usage:
        limiter = AsyncRateLimiter(requests_per_minute=30)
        await limiter.acquire()
        response = await groq_client.chat(...)
    """
    
    def __init__(self, requests_per_minute: float = 30.0, burst_size: Optional[int] = None):
        self.rate = requests_per_minute / 60.0  # tokens per second
        self.capacity = burst_size if burst_size else int(requests_per_minute)
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()
        
        # Metrics
        self.total_requests = 0
        self.total_wait_time = 0.0
        self.queue_length = 0
        
    async def acquire(self) -> float:
        """
        Acquire a token, waiting if necessary.

        Returns:
            Wait time in seconds (0 if no wait)

        Race-condition fix
        ------------------
        The original two-lock pattern had a bug: multiple coroutines that all
        found an empty bucket would each sleep independently, then all proceed
        in their second lock acquisition *without* consuming a token.  Under
        concurrent probe runs this effectively disabled rate limiting.

        The fix uses a single re-entrant check: after sleeping, re-enter the
        lock and subtract a token (refilled by elapsed time).  If the bucket
        is still empty (another waiter drained it first), sleep again.  This
        loop ensures exactly one token is consumed per successful acquire.
        """
        self.queue_length += 1
        total_wait = 0.0

        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    self.queue_length -= 1
                    self.total_requests += 1
                    self.total_wait_time += total_wait
                    return total_wait

                # Not enough tokens — calculate how long until one refills
                wait_time = (1 - self.tokens) / self.rate

            # Release lock while sleeping so other coroutines can check
            await asyncio.sleep(wait_time)
            total_wait += wait_time
    
    def get_metrics(self) -> Dict[str, Any]:
        """Return rate limiter performance metrics."""
        return {
            "total_requests": self.total_requests,
            "avg_wait_time_ms": (self.total_wait_time / max(1, self.total_requests)) * 1000,
            "current_queue": self.queue_length,
            "tokens_available": self.tokens,
            "capacity": self.capacity,
        }

    async def __aenter__(self) -> "AsyncRateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, *args: object) -> None:
        pass