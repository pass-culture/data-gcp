"""
Custom exception hierarchy for HTTP operations.
Enables classification of errors and smart retry logic.
"""

from typing import Optional


class HttpClientError(Exception):
    """Base exception for all HTTP client errors."""

    def __init__(
        self, message: str, url: str, original_error: Optional[Exception] = None
    ):
        self.url = url
        self.original_error = original_error
        super().__init__(message)

    def is_retryable(self) -> bool:
        """Override in subclasses to indicate if error is retryable."""
        return False


class NetworkError(HttpClientError):
    """Network-level errors (DNS, connection timeout, etc.)."""

    def is_retryable(self) -> bool:
        return True  # Network errors are usually transient


class HttpError(HttpClientError):
    """HTTP protocol errors (4xx, 5xx status codes)."""

    def __init__(
        self,
        message: str,
        url: str,
        status_code: int,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message, url, original_error)
        self.status_code = status_code

    def is_retryable(self) -> bool:
        # 5xx errors are retryable (server issues)
        # 429 is retryable (rate limit)
        # 4xx are not retryable (client errors) except 429
        return self.status_code >= 500 or self.status_code == 429


class RateLimitError(HttpError):
    """Rate limit exceeded (429)."""

    def __init__(
        self,
        message: str,
        url: str,
        retry_after: Optional[int] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message, url, 429, original_error)
        self.retry_after = retry_after

    def is_retryable(self) -> bool:
        return True


class AuthenticationError(HttpError):
    """Authentication errors (401, 403)."""

    def __init__(
        self,
        message: str,
        url: str,
        status_code: int,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message, url, status_code, original_error)

    def is_retryable(self) -> bool:
        # 401 retryable if we can refresh token
        # 403 not retryable (permission issue)
        return self.status_code == 401


class NotFoundError(HttpError):
    """Resource not found (404)."""

    def __init__(
        self, message: str, url: str, original_error: Optional[Exception] = None
    ):
        super().__init__(message, url, 404, original_error)

    def is_retryable(self) -> bool:
        return False  # 404 is permanent


class ServerError(HttpError):
    """Server errors (5xx)."""

    def is_retryable(self) -> bool:
        return True


def classify_http_error(
    status_code: int, url: str, original_error: Optional[Exception] = None
) -> HttpError:
    """Convert status code to appropriate exception type."""
    message = f"HTTP {status_code} error for {url}"

    if status_code == 404:
        return NotFoundError(message, url, original_error)
    elif status_code == 429:
        return RateLimitError(message, url, original_error=original_error)
    elif status_code in (401, 403):
        return AuthenticationError(message, url, status_code, original_error)
    elif status_code >= 500:
        return ServerError(message, url, status_code, original_error)
    else:
        return HttpError(message, url, status_code, original_error)


def parse_retry_after(response) -> Optional[int]:
    """
    Extract retry_after value from response headers.

    Args:
        response: HTTP response object (requests.Response or httpx.Response)

    Returns:
        Number of seconds to wait, or None if header not present
    """
    retry_after = response.headers.get("Retry-After")
    if retry_after is None:
        return None

    try:
        return int(retry_after)
    except ValueError:
        # Some APIs send HTTP-date string instead of seconds
        return None
