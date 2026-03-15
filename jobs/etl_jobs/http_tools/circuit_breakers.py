"""
Circuit breaker pattern for fault tolerance.
Prevents cascading failures by stopping requests to failing services.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Blocking all requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    failure_threshold: int = 5  # Open circuit after N failures
    success_threshold: int = 2  # Close circuit after N successes in half-open
    timeout_seconds: int = 60  # How long circuit stays open
    half_open_max_calls: int = 1  # Max concurrent calls in half-open state


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""

    pass


class CircuitBreaker:
    """
    Circuit breaker implementation.

    States:
    - CLOSED: Normal operation, all requests allowed
    - OPEN: Too many failures, block all requests
    - HALF_OPEN: Testing recovery, allow limited requests

    Usage:
        breaker = CircuitBreaker()

        if not breaker.allow_request():
            raise CircuitBreakerOpenError("Service unavailable")

        try:
            result = make_request()
            breaker.record_success()
            return result
        except Exception as e:
            breaker.record_failure()
            raise
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.half_open_calls = 0

    def allow_request(self) -> bool:
        """Check if request should be allowed."""

        if self.state == CircuitState.CLOSED:
            return True

        elif self.state == CircuitState.OPEN:
            # Check if timeout has elapsed
            if time.time() - self.last_failure_time >= self.config.timeout_seconds:
                logger.info("Circuit breaker transitioning to HALF_OPEN")
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                self.half_open_calls = 0
                return True
            else:
                logger.warning("Circuit breaker is OPEN, blocking request")
                return False

        else:  # HALF_OPEN
            # Allow limited concurrent calls
            if self.half_open_calls < self.config.half_open_max_calls:
                self.half_open_calls += 1
                return True
            else:
                logger.debug("Circuit breaker HALF_OPEN call limit reached")
                return False

    def record_success(self):
        """Record successful request."""

        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            self.half_open_calls = max(0, self.half_open_calls - 1)

            if self.success_count >= self.config.success_threshold:
                logger.info(
                    f"Circuit breaker closing after {self.success_count} successes"
                )
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0

        elif self.state == CircuitState.CLOSED:
            # Reset failure count on success
            self.failure_count = 0

    def record_failure(self):
        """Record failed request."""

        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            # Any failure in half-open → back to open
            logger.warning("Circuit breaker reopening after failure in HALF_OPEN state")
            self.state = CircuitState.OPEN
            self.failure_count = self.config.failure_threshold
            self.half_open_calls = max(0, self.half_open_calls - 1)

        elif self.state == CircuitState.CLOSED:
            self.failure_count += 1

            if self.failure_count >= self.config.failure_threshold:
                logger.error(
                    f"Circuit breaker opening after {self.failure_count} failures"
                )
                self.state = CircuitState.OPEN

    def reset(self):
        """Manually reset circuit breaker to closed state."""
        logger.info("Circuit breaker manually reset")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.half_open_calls = 0

    def get_state(self) -> str:
        """Get current circuit state as string."""
        return self.state.value


class PerEndpointCircuitBreaker:
    """
    Circuit breaker that tracks state per endpoint.
    Useful when different API endpoints have different reliability.

    Usage:
        breaker = PerEndpointCircuitBreaker()

        if not breaker.allow_request(url):
            raise CircuitBreakerOpenError(f"Circuit open for {url}")

        try:
            result = make_request(url)
            breaker.record_success(url)
        except Exception:
            breaker.record_failure(url)
            raise
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self._breakers = {}  # url -> CircuitBreaker

    def _get_breaker(self, endpoint: str) -> CircuitBreaker:
        """Get or create circuit breaker for endpoint."""
        if endpoint not in self._breakers:
            self._breakers[endpoint] = CircuitBreaker(self.config)
        return self._breakers[endpoint]

    def allow_request(self, endpoint: str) -> bool:
        """Check if request to endpoint should be allowed."""
        breaker = self._get_breaker(endpoint)
        return breaker.allow_request()

    def record_success(self, endpoint: str):
        """Record successful request to endpoint."""
        breaker = self._get_breaker(endpoint)
        breaker.record_success()

    def record_failure(self, endpoint: str):
        """Record failed request to endpoint."""
        breaker = self._get_breaker(endpoint)
        breaker.record_failure()

    def get_state(self, endpoint: str) -> str:
        """Get circuit state for endpoint."""
        breaker = self._get_breaker(endpoint)
        return breaker.get_state()

    def reset(self, endpoint: Optional[str] = None):
        """Reset circuit breaker(s)."""
        if endpoint:
            if endpoint in self._breakers:
                self._breakers[endpoint].reset()
        else:
            for breaker in self._breakers.values():
                breaker.reset()
