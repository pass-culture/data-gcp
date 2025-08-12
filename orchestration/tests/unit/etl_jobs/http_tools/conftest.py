"""HTTP Tools specific pytest fixtures."""

import pytest
import asyncio
import logging
from unittest.mock import Mock, AsyncMock
from http_tools.rate_limiters import BaseRateLimiter


# Configure logging for http_tools tests
logging.getLogger("http_tools").setLevel(logging.DEBUG)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_time():
    """Fixture for consistent time mocking across tests."""
    import time
    from unittest.mock import patch

    with patch("time.time") as mock:
        # Start at a known time
        mock.return_value = 1000.0
        yield mock


@pytest.fixture
def mock_response():
    """Create a mock HTTP response for testing."""
    response = Mock()
    response.status_code = 200
    response.ok = True
    response.reason = "OK"
    response.headers = {}
    return response


@pytest.fixture
def mock_rate_limit_response():
    """Create a mock 429 rate limit response."""
    response = Mock()
    response.status_code = 429
    response.ok = False
    response.reason = "Too Many Requests"
    response.headers = {"Retry-After": "60"}
    return response


@pytest.fixture
def mock_server_error_response():
    """Create a mock 500 server error response."""
    response = Mock()
    response.status_code = 500
    response.ok = False
    response.reason = "Internal Server Error"
    response.headers = {}
    return response


class MockSyncRateLimiter(BaseRateLimiter):
    """Mock synchronous rate limiter for testing."""

    def __init__(self):
        self.acquire_calls = 0
        self.backoff_calls = 0
        self.release_calls = 0
        self.should_wait = False
        self.acquire_should_raise = False
        self.backoff_should_raise = False

    def acquire(self):
        self.acquire_calls += 1
        if self.acquire_should_raise:
            raise Exception("Mock acquire error")
        if self.should_wait:
            import time

            time.sleep(0.01)  # Minimal wait for testing

    def backoff(self, response):
        self.backoff_calls += 1
        if self.backoff_should_raise:
            raise Exception("Mock backoff error")
        import time

        time.sleep(0.01)  # Minimal backoff for testing

    def release(self):
        self.release_calls += 1


class MockAsyncRateLimiter(BaseRateLimiter):
    """Mock asynchronous rate limiter for testing."""

    def __init__(self):
        self.acquire_calls = 0
        self.backoff_calls = 0
        self.release_calls = 0
        self.should_wait = False
        self.acquire_should_raise = False
        self.backoff_should_raise = False

    async def acquire(self):
        self.acquire_calls += 1
        if self.acquire_should_raise:
            raise Exception("Mock async acquire error")
        if self.should_wait:
            await asyncio.sleep(0.01)  # Minimal wait for testing

    async def backoff(self, response):
        self.backoff_calls += 1
        if self.backoff_should_raise:
            raise Exception("Mock async backoff error")
        await asyncio.sleep(0.01)  # Minimal backoff for testing

    def release(self):
        self.release_calls += 1


@pytest.fixture
def mock_sync_rate_limiter():
    """Provide a mock synchronous rate limiter."""
    return MockSyncRateLimiter()


@pytest.fixture
def mock_async_rate_limiter():
    """Provide a mock asynchronous rate limiter."""
    return MockAsyncRateLimiter()


@pytest.fixture
def sample_request_params():
    """Sample request parameters for testing."""
    return {
        "params": {"key": "value", "filter": "active"},
        "headers": {
            "Authorization": "Bearer token123",
            "Content-Type": "application/json",
            "User-Agent": "test-client/1.0",
        },
        "json": {"data": "test"},
        "timeout": 30,
    }


@pytest.fixture
def sensitive_request_params():
    """Request parameters with sensitive data for testing sanitization."""
    return {
        "params": {
            "api_key": "secret123",
            "token": "confidential",
            "password": "hidden",
            "normal_param": "visible",
        },
        "headers": {
            "Authorization": "Bearer secret_token",
            "X-API-Key": "another_secret",
            "X-Auth-Token": "auth_secret",
            "Content-Type": "application/json",  # This should be visible
        },
    }


@pytest.fixture
def performance_timer():
    """Utility for measuring test performance."""
    import time

    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.time()

        def stop(self):
            self.end_time = time.time()
            return self.elapsed

        @property
        def elapsed(self):
            if self.start_time is None or self.end_time is None:
                return None
            return self.end_time - self.start_time

    return Timer()


# Custom assertions for HTTP tools testing
def assert_rate_limiter_called(
    rate_limiter, acquire_count=None, backoff_count=None, release_count=None
):
    """Assert rate limiter methods were called the expected number of times."""
    if acquire_count is not None:
        assert (
            rate_limiter.acquire_calls == acquire_count
        ), f"Expected {acquire_count} acquire calls, got {rate_limiter.acquire_calls}"

    if backoff_count is not None:
        assert (
            rate_limiter.backoff_calls == backoff_count
        ), f"Expected {backoff_count} backoff calls, got {rate_limiter.backoff_calls}"

    if release_count is not None:
        assert (
            rate_limiter.release_calls == release_count
        ), f"Expected {release_count} release calls, got {rate_limiter.release_calls}"


# Make custom assertions available as pytest methods
pytest.assert_rate_limiter_called = assert_rate_limiter_called
