"""Factory for creating Titelive connectors with strategies."""

import logging

from connectors.titelive.auth import TiteliveAuthManager
from connectors.titelive.client import TiteliveConnector
from connectors.titelive.limiter import (
    TiteliveBasicRateLimiter,
    TiteliveBurstRecoveryLimiter,
)
from http_tools.circuit_breakers import CircuitBreaker, CircuitBreakerConfig
from http_tools.clients import SyncHttpClient
from http_tools.retry_strategies import RetryPolicy, create_retry_strategy

logger = logging.getLogger(__name__)


class TiteliveFactory:
    """
    Factory for creating Titelive connectors with configurable strategies.

    Supports:
    - Basic rate limiting (1 req/sec) for conservative mode
    - Burst-recovery rate limiting (70 req/s burst) for optimized mode
    - Enhanced retry strategies with exponential backoff
    - Circuit breaker for fault tolerance
    """

    @classmethod
    def create_connector(
        cls,
        project_id: str,
        use_burst_recovery: bool = False,
        use_enhanced_retry: bool = True,
        use_circuit_breaker: bool = True,
    ) -> TiteliveConnector:
        """
        Create a fully-configured Titelive connector.

        Args:
            project_id: GCP project ID for accessing secrets
            use_burst_recovery: Use burst-recovery rate limiter (Phase 2B)
            use_enhanced_retry: Enable enhanced retry strategies
            use_circuit_breaker: Enable circuit breaker

        Returns:
            Configured TiteliveConnector instance
        """
        logger.info("🏭 Building Titelive connector via factory")

        # 1. Build Auth Manager (with auto-refresh)
        auth = TiteliveAuthManager(project_id=project_id)
        logger.debug("✅ Auth manager configured (auto-refresh enabled)")

        # 2. Build Rate Limiter
        if use_burst_recovery:
            # Phase 2B: Burst-recovery pattern
            limiter = TiteliveBurstRecoveryLimiter()
            logger.info("📊 Rate limiter: Burst-Recovery (70 req/s burst)")
        else:
            # Phase 1: Conservative rate limiting
            limiter = TiteliveBasicRateLimiter()
            logger.info("📊 Rate limiter: Basic (1 req/sec)")

        # 3. Build Retry Strategy
        retry_strategy = None
        if use_enhanced_retry:
            retry_policy = RetryPolicy(
                max_retries=3,
                backoff_strategy="exponential",
                base_backoff_seconds=2.0,  # 2s, 4s, 8s
                max_backoff_seconds=60.0,
                jitter=True,
                retryable_status_codes=[0, 503, 504],  # Network, server errors
            )
            retry_strategy = create_retry_strategy(retry_policy)
            logger.debug("🔄 Retry strategy: Exponential backoff (3 retries)")

        # 4. Build Circuit Breaker
        circuit_breaker = None
        if use_circuit_breaker:
            circuit_config = CircuitBreakerConfig(
                failure_threshold=5,  # Open after 5 consecutive failures
                timeout_seconds=30,  # Stay open for 30 seconds
                success_threshold=2,  # Close after 2 successes in half-open
            )
            circuit_breaker = CircuitBreaker(circuit_config)
            logger.debug("🔌 Circuit breaker enabled (threshold=5)")

        # 5. Assemble HTTP Client
        client = SyncHttpClient(
            rate_limiter=limiter,
            auth_manager=auth,
            retry_strategy=retry_strategy,
            circuit_breaker=circuit_breaker,
        )

        # 6. Return Connector
        connector = TiteliveConnector(client=client)
        logger.info("✅ Titelive connector ready")

        return connector
