import logging

from connectors.brevo.auth import BrevoAuthManager
from connectors.brevo.client import AsyncBrevoConnector, BrevoConnector
from connectors.brevo.limiter import AsyncBrevoRateLimiter, SyncBrevoRateLimiter
from http_tools.circuit_breakers import CircuitBreaker, CircuitBreakerConfig
from http_tools.clients import AsyncHttpClient, SyncHttpClient
from http_tools.retry_strategies import RetryPolicy, create_retry_strategy
from utils.secrets import access_secret_data
from workflows.brevo.config import GCP_PROJECT, get_api_configuration

logger = logging.getLogger(__name__)


class BrevoFactory:
    @classmethod
    def create_connector(
        cls,
        audience: str,
        is_async: bool = False,
        max_concurrent: int = 5,
        use_enhanced_retry: bool = True,  # Enable new retry strategies
        use_circuit_breaker: bool = True,  # Enable circuit breaker
    ):
        logger.info(f"🏭 Building Brevo connector via factory (audience={audience})")

        # 1. Get configuration strings from config.py
        secret_name, _ = get_api_configuration(audience)

        # 2. Fetch the actual key from Secret Manager
        api_key = access_secret_data(GCP_PROJECT, secret_name)

        # 3. Build Auth Manager
        auth = BrevoAuthManager(api_key)
        logger.debug("✅ Auth manager configured (API Key)")

        # 4. Build Retry Strategy (if enabled)
        retry_strategy = None
        if use_enhanced_retry:
            # Use header-based retry for Brevo (respects Retry-After header)
            retry_policy = RetryPolicy(
                max_retries=5,  # More retries than legacy (3)
                backoff_strategy="header",  # Use Retry-After header
                base_backoff_seconds=10.0,  # Fallback if no header
                max_backoff_seconds=120.0,
                jitter=True,  # Add jitter to prevent thundering herd
                retryable_status_codes=[429, 502, 503, 504],  # Retry these codes
            )
            retry_strategy = create_retry_strategy(retry_policy)
            logger.debug(
                "🔄 Retry strategy: Header-based (respects Retry-After, 5 retries)"
            )

        # 5. Build Circuit Breaker (if enabled)
        circuit_breaker = None
        if use_circuit_breaker:
            circuit_config = CircuitBreakerConfig(
                failure_threshold=10,  # Open after 10 consecutive failures
                timeout_seconds=120,  # Stay open for 2 minutes
                success_threshold=2,  # Close after 2 successes in half-open
            )
            circuit_breaker = CircuitBreaker(circuit_config)
            logger.debug("🔌 Circuit breaker enabled (threshold=10, timeout=120s)")

        # 6. Assemble client with all strategies
        if is_async:
            limiter = AsyncBrevoRateLimiter(max_concurrent=max_concurrent)
            logger.info(f"📊 Rate limiter: Async (max_concurrent={max_concurrent})")
            client = AsyncHttpClient(
                rate_limiter=limiter,
                auth_manager=auth,
                retry_strategy=retry_strategy,
                circuit_breaker=circuit_breaker,
            )
            logger.info("✅ Brevo async connector ready")
            return AsyncBrevoConnector(client=client)

        limiter = SyncBrevoRateLimiter()
        logger.info("📊 Rate limiter: Sync (reactive)")
        client = SyncHttpClient(
            rate_limiter=limiter,
            auth_manager=auth,
            retry_strategy=retry_strategy,
            circuit_breaker=circuit_breaker,
        )
        logger.info("✅ Brevo connector ready")
        return BrevoConnector(client=client)
