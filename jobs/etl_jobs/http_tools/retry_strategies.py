"""
Retry strategies for handling transient failures.
Pluggable into HTTP clients or any other component.
"""

import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

from http_tools.exceptions import HttpClientError, RateLimitError

logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""

    max_retries: int = 3
    retryable_status_codes: List[int] = field(
        default_factory=lambda: [429, 502, 503, 504]
    )
    backoff_strategy: str = "exponential"  # "exponential", "linear", "header", "fixed"
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retry_on_network_errors: bool = True


class BaseRetryStrategy(ABC):
    """Abstract base for retry strategies."""

    def __init__(self, policy: Optional[RetryPolicy] = None):
        self.policy = policy or RetryPolicy()
        self._retry_count = 0

    @abstractmethod
    def should_retry(self, error: Exception) -> bool:
        """Determine if we should retry given an error."""
        pass

    @abstractmethod
    def calculate_backoff(self, error: Exception) -> float:
        """Calculate how long to wait before retrying."""
        pass

    def reset(self):
        """Reset retry counter (call after successful request)."""
        self._retry_count = 0

    def increment_retry_count(self):
        """Increment and return current retry count."""
        self._retry_count += 1
        return self._retry_count

    def get_retry_count(self) -> int:
        """Get current retry count."""
        return self._retry_count

    def _add_jitter(self, backoff: float) -> float:
        """Add random jitter to prevent thundering herd."""
        if self.policy.jitter:
            jitter_amount = random.uniform(0, backoff * 0.1)  # 0-10% jitter
            return backoff + jitter_amount
        return backoff


class ExponentialBackoffRetryStrategy(BaseRetryStrategy):
    """Retry with exponential backoff: 1s, 2s, 4s, 8s, ..."""

    def should_retry(self, error: Exception) -> bool:
        if self._retry_count >= self.policy.max_retries:
            return False

        if isinstance(error, HttpClientError):
            return error.is_retryable()

        # Retry on network errors if configured
        return self.policy.retry_on_network_errors

    def calculate_backoff(self, error: Exception) -> float:
        base = self.policy.base_backoff_seconds
        multiplier = self.policy.backoff_multiplier

        backoff = min(
            base * (multiplier**self._retry_count), self.policy.max_backoff_seconds
        )

        return self._add_jitter(backoff)


class HeaderBasedRetryStrategy(BaseRetryStrategy):
    """Retry based on Retry-After header (for 429 responses)."""

    def should_retry(self, error: Exception) -> bool:
        if self._retry_count >= self.policy.max_retries:
            return False

        if isinstance(error, RateLimitError):
            return True

        if isinstance(error, HttpClientError):
            return error.is_retryable()

        return False

    def calculate_backoff(self, error: Exception) -> float:
        # If error has retry_after, use it
        if isinstance(error, RateLimitError) and error.retry_after:
            return float(error.retry_after)

        # Otherwise fall back to exponential
        base = self.policy.base_backoff_seconds
        backoff = min(base * (2**self._retry_count), self.policy.max_backoff_seconds)

        return self._add_jitter(backoff)


class LinearBackoffRetryStrategy(BaseRetryStrategy):
    """Retry with linear backoff: 1s, 2s, 3s, 4s, ..."""

    def should_retry(self, error: Exception) -> bool:
        if self._retry_count >= self.policy.max_retries:
            return False

        if isinstance(error, HttpClientError):
            return error.is_retryable()

        return self.policy.retry_on_network_errors

    def calculate_backoff(self, error: Exception) -> float:
        backoff = min(
            self.policy.base_backoff_seconds * (self._retry_count + 1),
            self.policy.max_backoff_seconds,
        )

        return self._add_jitter(backoff)


class FixedBackoffRetryStrategy(BaseRetryStrategy):
    """Retry with fixed delay: 5s, 5s, 5s, ..."""

    def should_retry(self, error: Exception) -> bool:
        if self._retry_count >= self.policy.max_retries:
            return False

        if isinstance(error, HttpClientError):
            return error.is_retryable()

        return self.policy.retry_on_network_errors

    def calculate_backoff(self, error: Exception) -> float:
        return self._add_jitter(self.policy.base_backoff_seconds)


def create_retry_strategy(policy: Optional[RetryPolicy] = None) -> BaseRetryStrategy:
    """Create retry strategy based on policy configuration."""
    policy = policy or RetryPolicy()

    strategy_map = {
        "exponential": ExponentialBackoffRetryStrategy,
        "linear": LinearBackoffRetryStrategy,
        "fixed": FixedBackoffRetryStrategy,
        "header": HeaderBasedRetryStrategy,
    }

    strategy_class = strategy_map.get(
        policy.backoff_strategy, ExponentialBackoffRetryStrategy
    )
    return strategy_class(policy)
