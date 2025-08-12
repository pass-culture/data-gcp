import asyncio
import pytest
import time
from unittest.mock import Mock, MagicMock, patch, AsyncMock, call
import requests
import httpx

from http_tools.clients import (
    SyncHttpClient,
    AsyncHttpClient,
    create_sync_client,
    create_async_client,
)
from http_tools.rate_limiters import BaseRateLimiter, SimpleRateLimiter


class MockRateLimiter(BaseRateLimiter):
    """Mock rate limiter for testing."""

    def __init__(self):
        self.acquire_calls = 0
        self.backoff_calls = 0
        self.release_calls = 0
        self.should_wait = False

    def acquire(self):
        self.acquire_calls += 1
        if self.should_wait:
            time.sleep(0.01)  # Minimal wait

    def backoff(self, response):
        self.backoff_calls += 1
        time.sleep(0.01)  # Minimal backoff

    def release(self):
        self.release_calls += 1


class MockAsyncRateLimiter(BaseRateLimiter):
    """Mock async rate limiter for testing."""

    def __init__(self):
        self.acquire_calls = 0
        self.backoff_calls = 0
        self.release_calls = 0
        self.should_wait = False

    async def acquire(self):
        self.acquire_calls += 1
        if self.should_wait:
            await asyncio.sleep(0.01)

    async def backoff(self, response):
        self.backoff_calls += 1
        await asyncio.sleep(0.01)

    def release(self):
        self.release_calls += 1


class TestSyncHttpClient:
    def test_init_default(self):
        """Test SyncHttpClient initialization with defaults."""
        client = SyncHttpClient()

        assert client.rate_limiter is None
        assert client.timeout == 30
        assert client.max_retries == 3
        assert client.retry_status_codes == [500, 502, 503, 504]
        assert client.backoff_factor == 2.0
        assert client._request_count == 0
        assert hasattr(client, "session")

    def test_init_custom(self):
        """Test SyncHttpClient initialization with custom parameters."""
        rate_limiter = MockRateLimiter()

        client = SyncHttpClient(
            rate_limiter=rate_limiter,
            timeout=60,
            max_retries=5,
            retry_status_codes=[500, 502],
            backoff_factor=1.5,
        )

        assert client.rate_limiter == rate_limiter
        assert client.timeout == 60
        assert client.max_retries == 5
        assert client.retry_status_codes == [500, 502]
        assert client.backoff_factor == 1.5

    def test_set_rate_limiter(self):
        """Test setting rate limiter after initialization."""
        client = SyncHttpClient()
        rate_limiter = MockRateLimiter()

        client.set_rate_limiter(rate_limiter)

        assert client.rate_limiter == rate_limiter

    @patch("http_tools.clients.requests.Session.request")
    def test_successful_request(self, mock_request):
        """Test successful HTTP request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.reason = "OK"
        mock_request.return_value = mock_response

        client = SyncHttpClient()
        result = client.request("GET", "http://example.com")

        assert result == mock_response
        assert client._request_count == 1
        mock_request.assert_called_once_with("GET", "http://example.com", timeout=30)

    @patch("http_tools.clients.requests.Session.request")
    def test_request_with_rate_limiter(self, mock_request):
        """Test request with rate limiter integration."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        rate_limiter = MockRateLimiter()
        client = SyncHttpClient(rate_limiter=rate_limiter)

        result = client.request("GET", "http://example.com")

        assert result == mock_response
        assert rate_limiter.acquire_calls == 1
        # Release should be called (if the method exists)
        assert rate_limiter.release_calls == 1

    @patch("http_tools.clients.requests.Session.request")
    def test_request_429_rate_limit_retry(self, mock_request):
        """Test handling of 429 rate limit responses."""
        # First call returns 429, second returns 200
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.ok = False

        success_response = Mock()
        success_response.status_code = 200
        success_response.ok = True

        mock_request.side_effect = [rate_limit_response, success_response]

        rate_limiter = MockRateLimiter()
        client = SyncHttpClient(rate_limiter=rate_limiter)

        result = client.request("GET", "http://example.com")

        assert result == success_response
        assert mock_request.call_count == 2
        assert rate_limiter.backoff_calls == 1

    @patch("http_tools.clients.requests.Session.request")
    def test_request_429_max_retries_exceeded(self, mock_request):
        """Test 429 handling when max retries exceeded."""
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.ok = False

        # Always return 429
        mock_request.return_value = rate_limit_response

        rate_limiter = MockRateLimiter()
        client = SyncHttpClient(rate_limiter=rate_limiter)

        result = client.request("GET", "http://example.com")

        # Should eventually give up and return the 429
        assert result == rate_limit_response
        assert rate_limiter.backoff_calls == 3  # Max retries

    @patch("http_tools.clients.requests.Session.request")
    def test_timeout_error(self, mock_request):
        """Test timeout error handling."""
        mock_request.side_effect = requests.exceptions.Timeout("Timeout")

        client = SyncHttpClient()
        result = client.request("GET", "http://example.com")

        assert result is None

    @patch("http_tools.clients.requests.Session.request")
    def test_connection_error(self, mock_request):
        """Test connection error handling."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        client = SyncHttpClient()
        result = client.request("GET", "http://example.com")

        assert result is None

    @patch("http_tools.clients.requests.Session.request")
    def test_generic_request_exception(self, mock_request):
        """Test generic request exception handling."""
        mock_request.side_effect = requests.exceptions.RequestException("Generic error")

        client = SyncHttpClient()
        result = client.request("GET", "http://example.com")

        assert result is None

    @patch("http_tools.clients.requests.Session.request")
    def test_unexpected_exception(self, mock_request):
        """Test unexpected exception handling."""
        mock_request.side_effect = ValueError("Unexpected error")

        client = SyncHttpClient()
        result = client.request("GET", "http://example.com")

        assert result is None

    def test_get_stats(self):
        """Test statistics tracking."""
        client = SyncHttpClient(timeout=45, max_retries=5)

        stats = client.get_stats()

        expected = {"total_requests": 0, "timeout": 45, "max_retries": 5}
        assert stats == expected

    @patch("http_tools.clients.requests.Session.request")
    def test_request_params_and_headers(self, mock_request):
        """Test request with params and headers."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        client = SyncHttpClient()

        params = {"key": "value"}
        headers = {"Authorization": "Bearer token"}

        client.request("GET", "http://example.com", params=params, headers=headers)

        mock_request.assert_called_once_with(
            "GET", "http://example.com", params=params, headers=headers, timeout=30
        )

    def test_log_request_sanitization(self):
        """Test that sensitive data is sanitized in logs."""
        client = SyncHttpClient()

        # Test with sensitive parameters and headers
        params = {"token": "secret123", "normal": "value"}
        headers = {"Authorization": "Bearer secret", "Content-Type": "application/json"}

        # Should not raise exception
        client._log_request(1, "GET", "http://example.com", params, headers)

    @patch("http_tools.clients.requests.Session.request")
    def test_log_response(self, mock_request):
        """Test response logging."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"X-RateLimit-Remaining": "99"}
        mock_request.return_value = mock_response

        client = SyncHttpClient()

        # Should not raise exception
        client._log_response(1, "GET", "http://example.com", mock_response, 0.5)


class TestAsyncHttpClient:
    def test_init_default(self):
        """Test AsyncHttpClient initialization with defaults."""
        client = AsyncHttpClient()

        assert client.rate_limiter is None
        assert client.timeout == 30.0
        assert client.max_keepalive_connections == 20
        assert client.max_connections == 50
        assert client.max_retries == 3
        assert client.retry_status_codes == {500, 502, 503, 504}
        assert client.backoff_factor == 2.0
        assert client._request_count == 0
        assert client._client is None

    def test_init_custom(self):
        """Test AsyncHttpClient initialization with custom parameters."""
        rate_limiter = MockAsyncRateLimiter()

        client = AsyncHttpClient(
            rate_limiter=rate_limiter,
            timeout=60.0,
            max_keepalive_connections=10,
            max_connections=25,
            max_retries=5,
            retry_status_codes=[500, 502],
            backoff_factor=1.5,
        )

        assert client.rate_limiter == rate_limiter
        assert client.timeout == 60.0
        assert client.max_keepalive_connections == 10
        assert client.max_connections == 25
        assert client.max_retries == 5
        assert client.retry_status_codes == {500, 502}
        assert client.backoff_factor == 1.5

    @pytest.mark.asyncio
    async def test_get_client_creation(self):
        """Test httpx client creation."""
        client = AsyncHttpClient()

        httpx_client = client._get_client()

        assert isinstance(httpx_client, httpx.AsyncClient)
        assert client._client == httpx_client

        # Second call should return same instance
        httpx_client2 = client._get_client()
        assert httpx_client2 == httpx_client

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_successful_request(self, mock_request):
        """Test successful async HTTP request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        client = AsyncHttpClient()
        result = await client.request("GET", "http://example.com")

        assert result == mock_response
        assert client._request_count == 1
        mock_request.assert_called_once_with("GET", "http://example.com")

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_request_with_async_rate_limiter(self, mock_request):
        """Test async request with rate limiter integration."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        rate_limiter = MockAsyncRateLimiter()
        client = AsyncHttpClient(rate_limiter=rate_limiter)

        result = await client.request("GET", "http://example.com")

        assert result == mock_response
        assert rate_limiter.acquire_calls == 1

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_request_with_sync_rate_limiter(self, mock_request):
        """Test async client with sync rate limiter."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        rate_limiter = MockRateLimiter()  # Sync rate limiter
        client = AsyncHttpClient(rate_limiter=rate_limiter)

        result = await client.request("GET", "http://example.com")

        assert result == mock_response
        assert rate_limiter.acquire_calls == 1

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_request_429_retry(self, mock_request):
        """Test handling of 429 responses in async client."""
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.ok = False

        success_response = Mock()
        success_response.status_code = 200
        success_response.ok = True

        mock_request.side_effect = [rate_limit_response, success_response]

        rate_limiter = MockAsyncRateLimiter()
        client = AsyncHttpClient(rate_limiter=rate_limiter)

        result = await client.request("GET", "http://example.com")

        assert result == success_response
        assert mock_request.call_count == 2
        assert rate_limiter.backoff_calls == 1

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_server_error_retry(self, mock_request):
        """Test retry logic for server errors."""
        server_error = Mock()
        server_error.status_code = 500
        server_error.ok = False

        success_response = Mock()
        success_response.status_code = 200
        success_response.ok = True

        # First two calls fail, third succeeds
        mock_request.side_effect = [server_error, server_error, success_response]

        client = AsyncHttpClient()

        with patch("asyncio.sleep") as mock_sleep:
            result = await client.request("GET", "http://example.com")

        assert result == success_response
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2  # Two backoffs

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_timeout_error(self, mock_request):
        """Test timeout error handling in async client."""
        mock_request.side_effect = httpx.TimeoutException("Timeout")

        client = AsyncHttpClient(max_retries=1)

        with patch("asyncio.sleep") as mock_sleep:
            result = await client.request("GET", "http://example.com")

        assert result is None
        assert mock_request.call_count == 2  # Original + 1 retry
        assert mock_sleep.call_count == 1

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_connection_error(self, mock_request):
        """Test connection error handling in async client."""
        mock_request.side_effect = httpx.ConnectError("Connection failed")

        client = AsyncHttpClient(max_retries=1)

        with patch("asyncio.sleep") as mock_sleep:
            result = await client.request("GET", "http://example.com")

        assert result is None
        assert mock_request.call_count == 2  # Original + 1 retry

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_generic_http_error(self, mock_request):
        """Test generic HTTP error handling."""
        mock_request.side_effect = httpx.HTTPError("Generic HTTP error")

        client = AsyncHttpClient()
        result = await client.request("GET", "http://example.com")

        assert result is None

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_unexpected_exception(self, mock_request):
        """Test unexpected exception handling in async client."""
        mock_request.side_effect = ValueError("Unexpected error")

        client = AsyncHttpClient()
        result = await client.request("GET", "http://example.com")

        assert result is None

    @pytest.mark.asyncio
    async def test_close(self):
        """Test client cleanup."""
        client = AsyncHttpClient()

        # Create a client
        httpx_client = client._get_client()

        with patch.object(httpx_client, "aclose") as mock_close:
            await client.close()
            mock_close.assert_called_once()

        assert client._client is None

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager protocol."""
        async with AsyncHttpClient() as client:
            assert isinstance(client, AsyncHttpClient)

        # Client should be closed after context
        assert client._client is None

    def test_get_stats(self):
        """Test async client statistics."""
        client = AsyncHttpClient(timeout=60.0, max_retries=5)

        stats = client.get_stats()

        expected = {
            "total_requests": 0,
            "timeout": 60.0,
            "max_retries": 5,
            "retry_status_codes": [500, 502, 503, 504],  # Converted from set
        }
        assert stats == expected

    @pytest.mark.asyncio
    async def test_log_request_sanitization(self):
        """Test that sensitive data is sanitized in async logs."""
        client = AsyncHttpClient()

        # Test with sensitive parameters and headers
        params = {"secret": "password123", "normal": "value"}
        headers = {"Authorization": "Bearer secret", "Content-Type": "application/json"}

        # Should not raise exception
        client._log_request(1, "GET", "http://example.com", params, headers)

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_log_response(self, mock_request):
        """Test async response logging."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"X-RateLimit-Remaining": "99"}
        mock_request.return_value = mock_response

        client = AsyncHttpClient()

        # Should not raise exception
        client._log_response(1, "GET", "http://example.com", mock_response, 0.5, 0)


class TestFactoryFunctions:
    def test_create_sync_client_default(self):
        """Test sync client factory with defaults."""
        client = create_sync_client()

        assert isinstance(client, SyncHttpClient)
        assert client.rate_limiter is None

    def test_create_sync_client_with_rate_limiter(self):
        """Test sync client factory with rate limiter."""
        rate_limiter = SimpleRateLimiter()
        client = create_sync_client(rate_limiter=rate_limiter, timeout=60)

        assert isinstance(client, SyncHttpClient)
        assert client.rate_limiter == rate_limiter
        assert client.timeout == 60

    def test_create_async_client_default(self):
        """Test async client factory with defaults."""
        client = create_async_client()

        assert isinstance(client, AsyncHttpClient)
        assert client.rate_limiter is None

    def test_create_async_client_with_rate_limiter(self):
        """Test async client factory with rate limiter."""
        rate_limiter = MockAsyncRateLimiter()
        client = create_async_client(rate_limiter=rate_limiter, timeout=60.0)

        assert isinstance(client, AsyncHttpClient)
        assert client.rate_limiter == rate_limiter
        assert client.timeout == 60.0


# Integration tests
class TestClientRateLimiterIntegration:
    @patch("http_tools.clients.requests.Session.request")
    def test_sync_client_rate_limiter_flow(self, mock_request):
        """Test complete sync client + rate limiter integration."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        rate_limiter = MockRateLimiter()
        client = SyncHttpClient(rate_limiter=rate_limiter)

        # Make multiple requests
        for i in range(3):
            result = client.request("GET", f"http://example.com/{i}")
            assert result == mock_response

        assert rate_limiter.acquire_calls == 3
        assert rate_limiter.release_calls == 3
        assert client._request_count == 3

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_async_client_rate_limiter_flow(self, mock_request):
        """Test complete async client + rate limiter integration."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        rate_limiter = MockAsyncRateLimiter()
        client = AsyncHttpClient(rate_limiter=rate_limiter)

        try:
            # Make multiple requests
            for i in range(3):
                result = await client.request("GET", f"http://example.com/{i}")
                assert result == mock_response

            assert rate_limiter.acquire_calls == 3
            assert client._request_count == 3
        finally:
            await client.close()

    @patch("http_tools.clients.requests.Session.request")
    def test_rate_limiter_backoff_integration(self, mock_request):
        """Test rate limiter backoff integration with sync client."""
        # Simulate rate limit then success
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.ok = False

        success_response = Mock()
        success_response.status_code = 200
        success_response.ok = True

        mock_request.side_effect = [rate_limit_response, success_response]

        rate_limiter = MockRateLimiter()
        client = SyncHttpClient(rate_limiter=rate_limiter)

        result = client.request("GET", "http://example.com")

        assert result == success_response
        assert rate_limiter.acquire_calls == 2  # Initial + retry
        assert rate_limiter.backoff_calls == 1
        assert rate_limiter.release_calls == 2


class TestErrorScenarios:
    @patch("http_tools.clients.requests.Session.request")
    def test_rate_limiter_exception_handling(self, mock_request):
        """Test client handles rate limiter exceptions gracefully."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        # Mock rate limiter that raises exception
        rate_limiter = Mock()
        rate_limiter.acquire.side_effect = Exception("Rate limiter failed")
        rate_limiter.release.side_effect = Exception("Release failed")

        client = SyncHttpClient(rate_limiter=rate_limiter)

        # Should handle exception and return None
        result = client.request("GET", "http://example.com")
        assert result is None

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient.request")
    async def test_async_rate_limiter_exception_handling(self, mock_request):
        """Test async client handles rate limiter exceptions."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_request.return_value = mock_response

        # Mock async rate limiter that raises exception
        rate_limiter = Mock()
        rate_limiter.acquire = AsyncMock(
            side_effect=Exception("Async rate limiter failed")
        )

        client = AsyncHttpClient(rate_limiter=rate_limiter)

        try:
            result = await client.request("GET", "http://example.com")
            assert result is None
        finally:
            await client.close()
