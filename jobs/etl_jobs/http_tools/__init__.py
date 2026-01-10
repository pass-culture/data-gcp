"""
HTTP Tools for ETL Jobs.

This package provides reusable infrastructure for HTTP-based data extraction:
- Base HTTP clients (sync and async)
- Authentication managers
- Rate limiters
- Retry strategies
- Circuit breakers
- Custom exceptions
"""

# Base clients
# Authentication
from http_tools.auth import BaseAuthManager, retry_on_401

# Circuit breakers
from http_tools.circuit_breakers import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
    PerEndpointCircuitBreaker,
)
from http_tools.clients import AsyncHttpClient, BaseHttpClient, SyncHttpClient

# Exceptions
from http_tools.exceptions import (
    AuthenticationError,
    HttpClientError,
    HttpError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    ServerError,
    classify_http_error,
    parse_retry_after,
)

# Rate limiting
from http_tools.rate_limiters import (
    AsyncBaseRateLimiter,
    AsyncTokenBucketRateLimiter,
    BaseRateLimiter,
    SyncBaseRateLimiter,
    SyncTokenBucketRateLimiter,
)

# Retry strategies
from http_tools.retry_strategies import (
    BaseRetryStrategy,
    ExponentialBackoffRetryStrategy,
    FixedBackoffRetryStrategy,
    HeaderBasedRetryStrategy,
    LinearBackoffRetryStrategy,
    RetryPolicy,
    create_retry_strategy,
)

__all__ = [
    # Clients
    "BaseHttpClient",
    "SyncHttpClient",
    "AsyncHttpClient",
    # Auth
    "BaseAuthManager",
    "retry_on_401",
    # Rate limiters
    "BaseRateLimiter",
    "SyncBaseRateLimiter",
    "AsyncBaseRateLimiter",
    "SyncTokenBucketRateLimiter",
    "AsyncTokenBucketRateLimiter",
    # Retry strategies
    "BaseRetryStrategy",
    "RetryPolicy",
    "ExponentialBackoffRetryStrategy",
    "LinearBackoffRetryStrategy",
    "FixedBackoffRetryStrategy",
    "HeaderBasedRetryStrategy",
    "create_retry_strategy",
    # Circuit breakers
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitState",
    "PerEndpointCircuitBreaker",
    # Exceptions
    "HttpClientError",
    "NetworkError",
    "HttpError",
    "RateLimitError",
    "AuthenticationError",
    "NotFoundError",
    "ServerError",
    "classify_http_error",
    "parse_retry_after",
]
