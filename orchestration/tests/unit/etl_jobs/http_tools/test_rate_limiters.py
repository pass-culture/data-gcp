"""Unit tests for HTTP Tools rate limiters."""

import asyncio
import pytest
import time
import threading
from unittest.mock import patch, MagicMock

from http_tools.rate_limiters import (
    SimpleRateLimiter,
    TokenBucketRateLimiter,
    AsyncTokenBucketRateLimiter,
    NoOpRateLimiter,
)


@pytest.mark.unit
class TestSimpleRateLimiter:
    """Test suite for SimpleRateLimiter."""

    def test_init(self):
        """Test SimpleRateLimiter initialization."""
        limiter = SimpleRateLimiter(requests_per_minute=120)
        assert limiter.requests_per_minute == 120
        assert limiter.min_interval == 0.5  # 60/120

    def test_zero_rate_limit_allows_unlimited(self):
        """Test that zero rate limit allows unlimited requests."""
        limiter = SimpleRateLimiter(requests_per_minute=0)
        assert limiter.min_interval == 0

        # Should not wait at all
        start = time.time()
        limiter.acquire()
        limiter.acquire()
        elapsed = time.time() - start
        assert elapsed < 0.1  # Should be essentially instant

    @patch("time.sleep")
    @patch("time.time")
    def test_acquire_respects_timing(self, mock_time, mock_sleep):
        """Test acquire respects timing intervals."""
        # Setup time progression: 0, 0.3, 0.3, 1.0
        mock_time.side_effect = [0, 0.3, 0.3, 1.0]

        limiter = SimpleRateLimiter(requests_per_minute=60)  # 1 req/second

        # First request - should not wait
        limiter.acquire()
        mock_sleep.assert_not_called()

        # Second request too soon - should wait
        limiter.acquire()
        mock_sleep.assert_called_once_with(0.7)  # 1.0 - 0.3

    def test_backoff(self):
        """Test backoff functionality."""
        limiter = SimpleRateLimiter()

        with patch("time.sleep") as mock_sleep:
            response = MagicMock()
            limiter.backoff(response)
            mock_sleep.assert_called_once_with(60)


@pytest.mark.unit
class TestTokenBucketRateLimiter:
    """Test suite for TokenBucketRateLimiter."""

    def test_init(self):
        """Test TokenBucketRateLimiter initialization."""
        limiter = TokenBucketRateLimiter(
            requests_per_minute=120, burst_capacity=10, window_size=60
        )
        assert limiter.requests_per_minute == 120
        assert limiter.burst_capacity == 10
        assert limiter.window_size == 60
        assert limiter._tokens == 10

    def test_default_burst_capacity(self):
        """Test default burst capacity equals requests_per_minute."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60)
        assert limiter.burst_capacity == 60

    @patch("time.time")
    @patch("time.sleep")
    def test_acquire_with_available_tokens(self, mock_sleep, mock_time):
        """Test acquire when tokens are available."""
        mock_time.return_value = 100.0

        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_capacity=5)
        limiter._last_update = 100.0
        limiter._tokens = 3.0

        limiter.acquire()

        mock_sleep.assert_not_called()
        assert limiter._tokens == 2.0

    @patch("time.time")
    @patch("time.sleep")
    def test_token_refill_over_time(self, mock_sleep, mock_time):
        """Test token refill over time."""
        # Time progresses from 100 to 160 (60 seconds)
        mock_time.side_effect = [160.0, 160.0]

        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_capacity=5)
        limiter._last_update = 100.0  # 60 seconds ago
        limiter._tokens = 0.0

        limiter.acquire()

        mock_sleep.assert_not_called()
        # Should have refilled to burst_capacity and used one token
        assert limiter._tokens == 4.0

    def test_backoff_with_retry_after_header(self):
        """Test backoff with Retry-After header."""
        limiter = TokenBucketRateLimiter()

        response = MagicMock()
        response.headers = {"Retry-After": "30"}

        with patch("time.sleep") as mock_sleep:
            with patch("time.time", return_value=100.0):
                limiter.backoff(response)

                mock_sleep.assert_called_once_with(30.0)
                assert limiter._tokens == limiter.burst_capacity


@pytest.mark.unit
@pytest.mark.asyncio
class TestAsyncTokenBucketRateLimiter:
    """Test suite for AsyncTokenBucketRateLimiter."""

    @pytest.fixture
    def async_limiter(self):
        """Create an AsyncTokenBucketRateLimiter for testing."""
        return AsyncTokenBucketRateLimiter(
            requests_per_minute=60, burst_capacity=5, max_concurrent=3
        )

    def test_init(self, async_limiter):
        """Test AsyncTokenBucketRateLimiter initialization."""
        assert async_limiter.requests_per_minute == 60
        assert async_limiter.burst_capacity == 5
        assert async_limiter.max_concurrent == 3
        assert async_limiter._tokens == 5
        assert async_limiter._semaphore._value == 3

    async def test_acquire_with_available_tokens(self, async_limiter):
        """Test acquire when tokens are available."""
        async_limiter._tokens = 3.0

        with patch("time.time", return_value=100.0):
            await async_limiter.acquire()

        assert async_limiter._tokens == 2.0
        assert async_limiter._semaphore._value == 2  # Semaphore acquired

    async def test_token_refill_async(self, async_limiter):
        """Test async token refill."""
        async_limiter._tokens = 0.0
        async_limiter._last_update = 40.0  # 60 seconds ago

        with patch("time.time", return_value=100.0):
            await async_limiter.acquire()

        assert async_limiter._tokens == 4.0  # Refilled to 5, used 1
        assert async_limiter._semaphore._value == 2

    async def test_backoff_async(self, async_limiter):
        """Test async backoff functionality."""
        response = MagicMock()
        response.headers = {"Retry-After": "15"}

        with patch("asyncio.sleep") as mock_sleep:
            with patch("time.time", return_value=100.0):
                await async_limiter.backoff(response)

                mock_sleep.assert_called_once_with(15.0)
                assert async_limiter._tokens == async_limiter.burst_capacity

    async def test_concurrent_request_limiting(self, async_limiter):
        """Test concurrent request limiting."""

        async def worker():
            await async_limiter.acquire()
            await asyncio.sleep(0.01)  # Simulate work
            async_limiter.release()

        # Start more tasks than max_concurrent
        tasks = [asyncio.create_task(worker()) for _ in range(5)]

        # Should complete without hanging
        await asyncio.gather(*tasks)

        # All semaphore slots should be available again
        assert async_limiter._semaphore._value == 3

    def test_release(self, async_limiter):
        """Test semaphore release."""
        # Manually acquire semaphore
        async_limiter._semaphore._value = 2

        async_limiter.release()

        assert async_limiter._semaphore._value == 3


@pytest.mark.unit
class TestNoOpRateLimiter:
    """Test suite for NoOpRateLimiter."""

    def test_acquire_is_instant(self):
        """Test no-op acquire is instant."""
        limiter = NoOpRateLimiter()

        start = time.time()
        limiter.acquire()
        elapsed = time.time() - start

        assert elapsed < 0.01  # Should be instant

    def test_backoff_has_minimal_delay(self):
        """Test minimal backoff delay."""
        limiter = NoOpRateLimiter()

        with patch("time.sleep") as mock_sleep:
            response = MagicMock()
            limiter.backoff(response)
            mock_sleep.assert_called_once_with(1)

    def test_release_does_nothing(self):
        """Test no-op release."""
        limiter = NoOpRateLimiter()
        # Should not raise any exceptions
        limiter.release()


@pytest.mark.unit
class TestRateLimiterInterface:
    """Test that all rate limiters implement the same interface."""

    def test_sync_limiters_have_same_interface(self):
        """Test that all sync limiters implement the same interface."""
        limiters = [
            SimpleRateLimiter(requests_per_minute=60),
            TokenBucketRateLimiter(requests_per_minute=60),
            NoOpRateLimiter(),
        ]

        for limiter in limiters:
            # All should have these methods
            assert hasattr(limiter, "acquire")
            assert hasattr(limiter, "backoff")
            assert hasattr(limiter, "release")

            # Methods should be callable
            assert callable(limiter.acquire)
            assert callable(limiter.backoff)
            assert callable(limiter.release)

    @pytest.mark.asyncio
    async def test_async_limiter_interface(self):
        """Test async limiter implements required interface."""
        limiter = AsyncTokenBucketRateLimiter()

        # Should have async methods
        assert hasattr(limiter, "acquire")
        assert hasattr(limiter, "backoff")
        assert hasattr(limiter, "release")

        # Methods should be callable
        assert callable(limiter.acquire)
        assert callable(limiter.backoff)
        assert callable(limiter.release)

        # Test they actually work
        await limiter.acquire()
        response = MagicMock()
        response.headers = {}

        with patch("asyncio.sleep"):
            await limiter.backoff(response)

        limiter.release()


@pytest.mark.integration
class TestRateLimiterIntegration:
    """Integration tests for rate limiters."""

    @pytest.mark.slow
    def test_token_bucket_timing_integration(self):
        """Test token bucket timing in real conditions."""
        limiter = TokenBucketRateLimiter(
            requests_per_minute=120,  # 2 per second
            burst_capacity=3,
        )

        start_time = time.time()

        # Should allow burst of 3 requests immediately
        for _ in range(3):
            limiter.acquire()

        burst_time = time.time() - start_time
        assert burst_time < 0.1  # Burst should be fast

        # Fourth request should be delayed
        limiter.acquire()
        total_time = time.time() - start_time
        assert total_time >= 0.4  # Should wait for token refill

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_async_concurrent_access(self):
        """Test async rate limiter with concurrent access."""
        limiter = AsyncTokenBucketRateLimiter(
            requests_per_minute=60, burst_capacity=5, max_concurrent=3
        )

        results = []

        async def worker(worker_id):
            await limiter.acquire()
            results.append(worker_id)
            await asyncio.sleep(0.01)  # Simulate work
            limiter.release()

        # Start 10 workers
        tasks = [worker(i) for i in range(10)]
        await asyncio.gather(*tasks)

        # All workers should complete
        assert len(results) == 10
        assert set(results) == set(range(10))
