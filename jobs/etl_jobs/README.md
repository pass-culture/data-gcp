# ETL Jobs Architecture

A modular, production-ready ETL framework built on the **Factory Pattern** with pluggable strategies for HTTP communication, authentication, rate limiting, error handling, and retry logic.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Layer Design](#layer-design)
  - [Layer 1: HTTP Tools (Foundation)](#layer-1-http-tools-foundation)
  - [Layer 2: Connectors (API-Specific)](#layer-2-connectors-api-specific)
  - [Layer 3: Factories (Assembly)](#layer-3-factories-assembly)
  - [Layer 4: Jobs (Business Logic)](#layer-4-jobs-business-logic)
- [Cross-Cutting Concerns](#cross-cutting-concerns)
  - [Error Handling Philosophy](#error-handling-philosophy)
  - [Retry Strategies Philosophy](#retry-strategies-philosophy)
  - [Logging Philosophy](#logging-philosophy)
- [Implementation Guide: Adding New APIs](#implementation-guide-adding-new-apis)
- [Examples](#examples)
- [Testing Strategy](#testing-strategy)

---

## Architecture Overview

The codebase follows a **4-layer architecture** designed for:

- **Separation of Concerns**: Each layer has a single, well-defined responsibility
- **Reusability**: HTTP tools are API-agnostic and can be shared across connectors
- **Testability**: Each layer can be tested in isolation with mocks
- **Configurability**: Behavior is controlled via dependency injection, not hardcoding
- **Extensibility**: New APIs can be added without modifying existing code

```
┌─────────────────────────────────────────────────────────┐
│  Layer 4: Jobs (ETL Business Logic)                     │
│  ├─ Extract: Fetch data via connectors                  │
│  ├─ Transform: Clean, validate, enrich                  │
│  └─ Load: Save to BigQuery, GCS, etc.                   │
└──────────────────┬──────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────┐
│  Layer 3: Factories (Dependency Injection)              │
│  └─ Assembles connectors with strategies                │
│     (rate limiters, auth, retry, circuit breakers)      │
└──────────────────┬──────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────┐
│  Layer 2: Connectors (API-Specific Clients)             │
│  ├─ Client: Implements API routes/methods               │
│  ├─ Limiter: API-specific rate limiting strategy        │
│  ├─ Auth: API-specific authentication strategy          │
│  └─ Config: API keys, endpoints, schemas                │
└──────────────────┬──────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────┐
│  Layer 1: HTTP Tools (Foundation)                       │
│  ├─ Base Clients: Sync/Async HTTP with retry logic      │
│  ├─ Base Auth: OAuth2, API Key, JWT strategies          │
│  ├─ Base Rate Limiters: Token bucket, sliding window    │
│  ├─ Retry Strategies: Exponential, linear, header-based │
│  ├─ Circuit Breakers: Fault tolerance patterns          │
│  └─ Exceptions: Typed error hierarchy                   │
└─────────────────────────────────────────────────────────┘
```

---

## Layer Design

### Layer 1: HTTP Tools (Foundation)

**Location**: `http_tools/`

**Purpose**: Provide reusable, API-agnostic HTTP infrastructure.

#### Components

| Module | Purpose | Key Classes |
|--------|---------|-------------|
| `clients.py` | HTTP request execution | `SyncHttpClient`, `AsyncHttpClient` |
| `auth.py` | Authentication strategies | `BaseAuthManager`, `APIKeyAuthManager`, `OAuth2AuthManager` |
| `rate_limiters.py` | Proactive rate limiting | `BaseRateLimiter`, `TokenBucketLimiter`, `SlidingWindowLimiter` |
| `retry_strategies.py` | Retry policies | `ExponentialBackoffRetryStrategy`, `HeaderBasedRetryStrategy` |
| `circuit_breakers.py` | Fault tolerance | `CircuitBreaker`, `PerEndpointCircuitBreaker` |
| `exceptions.py` | Typed error hierarchy | `HttpClientError`, `RateLimitError`, `ServerError` |

#### Design Principles

- **Strategy Pattern**: Pluggable strategies for auth, rate limiting, retry
- **Open/Closed Principle**: Extend via new strategies, not modification
- **Type Safety**: Custom exceptions enable smart error handling
- **Backwards Compatibility**: Legacy retry logic preserved alongside new strategies

#### Key Features

**HTTP Clients**:
- Automatic auth token injection
- Proactive rate limiting with acquire/release
- Circuit breaker integration
- Retry strategies (exponential, linear, header-based)
- Sync and async modes

**Exception Hierarchy**:
```python
HttpClientError (base)
├─ NetworkError (retryable)
├─ HttpError
│  ├─ RateLimitError (429, retryable)
│  ├─ AuthenticationError (401/403)
│  ├─ NotFoundError (404)
│  └─ ServerError (5xx, retryable)
```

**Retry Strategies**:
- Exponential backoff: 1s → 2s → 4s → 8s (default)
- Linear backoff: 1s → 2s → 3s → 4s
- Fixed backoff: 5s → 5s → 5s
- Header-based: Respects `Retry-After` header (best for 429)
- Jitter support (prevents thundering herd)

**Circuit Breakers**:
- **CLOSED**: Normal operation (all requests allowed)
- **OPEN**: Too many failures (blocking requests)
- **HALF_OPEN**: Testing recovery (limited requests)

---

### Layer 2: Connectors (API-Specific)

**Location**: `connectors/{api_name}/`

**Purpose**: Implement API-specific behavior using HTTP tools.

#### Structure

Each API connector has:

```
connectors/
└── brevo/                    # Example: Brevo API
    ├── client.py             # API routes implementation
    ├── auth.py               # Brevo-specific auth (API key)
    ├── limiter.py            # Brevo-specific rate limiting
    ├── config.py             # API keys, endpoints
    └── README.md             # API-specific documentation
```

#### Responsibilities

| File | Responsibility | Example |
|------|----------------|---------|
| `client.py` | Implement API routes | `get_email_campaigns()`, `get_smtp_templates()` |
| `auth.py` | API authentication | `BrevoAuthManager` (API key in header) |
| `limiter.py` | API rate limiting | `BrevoRateLimiter` (uses `x-sib-ratelimit-*` headers) |
| `config.py` | Configuration | `BREVO_API_KEY`, `BASE_URL`, schemas |

#### Design Principles

- **Single Responsibility**: Each connector handles ONE API
- **Composition over Inheritance**: Uses base classes from `http_tools/`
- **Reactive Rate Limiting**: Adjusts based on API response headers
- **No Business Logic**: Pure HTTP/API concerns only

---

### Layer 3: Factories (Assembly)

**Location**: `factories/`

**Purpose**: Assemble connectors with strategies via dependency injection.

#### Example: `factories/brevo.py`

```python
class BrevoFactory:
    @staticmethod
    def create_connector(
        audience: str,
        is_async: bool = False,
        max_concurrent: int = 5,
        use_enhanced_retry: bool = True,
        use_circuit_breaker: bool = True,
    ) -> BrevoConnector:
        """
        Assembles a fully-configured Brevo connector.

        Strategies injected:
        - Auth: API Key via BrevoAuthManager
        - Rate Limiter: Reactive limiter using Brevo headers
        - Retry Strategy: Header-based (respects Retry-After)
        - Circuit Breaker: Opens after 10 failures, 2min timeout
        """
        # 1. Build auth manager
        auth = BrevoAuthManager(api_key=get_api_key(audience))

        # 2. Build retry strategy (if enabled)
        retry_strategy = None
        if use_enhanced_retry:
            policy = RetryPolicy(
                max_retries=5,
                backoff_strategy="header",  # Respects Retry-After
                base_backoff_seconds=10.0,
                jitter=True,
            )
            retry_strategy = create_retry_strategy(policy)

        # 3. Build circuit breaker (if enabled)
        circuit_breaker = None
        if use_circuit_breaker:
            config = CircuitBreakerConfig(
                failure_threshold=10,
                timeout_seconds=120,
            )
            circuit_breaker = CircuitBreaker(config)

        # 4. Build HTTP client
        if is_async:
            limiter = AsyncBrevoRateLimiter(max_concurrent)
            client = AsyncHttpClient(limiter, auth, retry_strategy, circuit_breaker)
        else:
            limiter = SyncBrevoRateLimiter()
            client = SyncHttpClient(limiter, auth, retry_strategy=retry_strategy, circuit_breaker=circuit_breaker)

        # 5. Return configured connector
        return BrevoConnector(client)
```

#### Design Principles

- **Dependency Injection**: Strategies are injected, not hardcoded
- **Single Entry Point**: Jobs only interact with factories
- **Configuration as Code**: Retry policies, circuit breaker thresholds in one place
- **Feature Flags**: Enable/disable strategies via parameters

---

### Layer 4: Jobs (Business Logic)

**Location**: `jobs/{api_name}/`

**Purpose**: Implement ETL workflows using connectors.

#### Structure

```
jobs/
└── brevo/
    ├── main.py              # CLI entry point (Typer)
    ├── tasks.py             # ETL tasks (extract logic)
    ├── transform.py         # Data transformation
    ├── load.py              # BigQuery/GCS loading
    └── config.py            # Job-specific config (schemas, tables)
```

#### Responsibilities

| File | Responsibility | Example |
|------|----------------|---------|
| `main.py` | Orchestration, CLI | Parse dates, dispatch tasks, handle top-level errors |
| `tasks.py` | Extract data | Fetch campaigns, handle pagination, graceful degradation |
| `transform.py` | Transform data | Clean, validate, enrich, convert to DataFrame |
| `load.py` | Load data | Save to BigQuery, handle schema evolution |

#### Key Patterns

**Graceful Degradation**:
```python
failed_templates = []
for template in templates:
    try:
        data = connector.get_template_data(template_id)
        all_events.extend(data)
    except CircuitBreakerOpenError:
        logger.error(f"Circuit breaker open for template {template_id}")
        failed_templates.append(template_id)
    except HttpClientError as e:
        logger.error(f"Failed to fetch template {template_id}: {e}")
        failed_templates.append(template_id)

# Log summary
logger.warning(f"Failed {len(failed_templates)} templates: {failed_templates}")

# Continue processing with partial data
if all_events:
    transform_and_load(all_events)
```

**Exception Propagation**:
```python
def run_newsletter_etl(connector, audience, table_name, end_date):
    try:
        resp = connector.get_email_campaigns()
    except CircuitBreakerOpenError:
        logger.error("🛑 Circuit breaker open for Brevo API")
        raise  # Propagate to main.py
    except RateLimitError as e:
        logger.error(f"🛑 Rate limit exceeded. Retry-After: {e.retry_after}s")
        raise
    except ServerError as e:
        logger.error(f"🛑 Server error: {e.status_code}")
        raise

    # Process data...
```

---

## Cross-Cutting Concerns

### Error Handling Philosophy

#### Principles

1. **Typed Exceptions**: Use custom exception hierarchy for smart error handling
2. **Fail Fast**: Propagate errors up, don't swallow
3. **Graceful Degradation**: Continue processing when partial success is acceptable
4. **Explicit Logging**: Log error details before re-raising
5. **Circuit Breaking**: Stop trying when system is down

#### Exception Classification

| Exception Type | Status Codes | Retryable? | Action |
|----------------|--------------|------------|--------|
| `NetworkError` | Connection errors | ✅ Yes | Retry with backoff |
| `RateLimitError` | 429 | ✅ Yes | Retry after `Retry-After` header |
| `ServerError` | 500-599 | ✅ Yes | Retry with exponential backoff |
| `AuthenticationError` | 401, 403 | ❌ No* | Refresh token once, then fail |
| `NotFoundError` | 404 | ❌ No | Fail immediately |
| `CircuitBreakerOpenError` | N/A | ❌ No | Wait for circuit to close |

\* *Auth errors trigger one token refresh attempt, then fail*

#### Error Flow

```
HTTP Client
├─ Network Error → NetworkError → Retry (exponential backoff)
├─ 429 → RateLimitError → Backoff (Retry-After header) → Retry
├─ 401 → AuthenticationError → Refresh token → Retry once
├─ 404 → NotFoundError → Raise immediately
├─ 5xx → ServerError → Retry (exponential backoff)
└─ Circuit Open → CircuitBreakerOpenError → Raise immediately

Task Function
├─ Log specific error details
└─ Raise (propagate to main.py)

Main Function
├─ Catch all exceptions
├─ Log critical error with stack trace
└─ Exit with code 1 (signals failure to orchestrator)
```

#### Best Practices

**DO**:
- Use typed exceptions for different error scenarios
- Log error details before re-raising
- Implement graceful degradation for batch operations
- Let circuit breakers prevent cascading failures

**DON'T**:
- Catch `Exception` unless you're at the top level
- Return `None` on errors (raise exceptions instead)
- Retry non-retryable errors (404, 403)
- Ignore circuit breaker open state

---

### Retry Strategies Philosophy

#### Principles

1. **Pluggable Strategies**: Choose strategy based on API behavior
2. **Respect API Signals**: Use `Retry-After` when provided
3. **Exponential Backoff**: Default for unknown APIs
4. **Jitter**: Add randomness to prevent thundering herd
5. **Max Retries**: Always have a limit (default: 3-5)

#### Strategy Selection Guide

| API Behavior | Recommended Strategy | Configuration |
|--------------|---------------------|---------------|
| **Provides `Retry-After` header** | `HeaderBasedRetryStrategy` | `backoff_strategy="header"` |
| **No retry headers** | `ExponentialBackoffRetryStrategy` | `backoff_strategy="exponential"` |
| **Very strict rate limits** | `LinearBackoffRetryStrategy` | `backoff_strategy="linear"` |
| **Simple retry logic** | `FixedBackoffRetryStrategy` | `backoff_strategy="fixed"` |

#### Configuration Example

```python
# Aggressive retry for tolerant APIs
aggressive_policy = RetryPolicy(
    max_retries=10,
    base_backoff_seconds=1.0,
    backoff_strategy="exponential",
    jitter=True,
)

# Conservative retry for strict APIs
conservative_policy = RetryPolicy(
    max_retries=3,
    base_backoff_seconds=10.0,
    backoff_strategy="linear",
    jitter=False,
)

# Header-based for APIs with Retry-After
header_policy = RetryPolicy(
    max_retries=5,
    backoff_strategy="header",  # Respects Retry-After
    base_backoff_seconds=10.0,  # Fallback if no header
    jitter=True,
)
```

#### Backoff Sequences

**Exponential** (default):
```
Attempt 1: 1.0s  (base)
Attempt 2: 2.0s  (base * 2^1)
Attempt 3: 4.0s  (base * 2^2)
Attempt 4: 8.0s  (base * 2^3)
Attempt 5: 16.0s (base * 2^4)
Max: 60.0s (capped)
```

**Linear**:
```
Attempt 1: 1.0s  (base)
Attempt 2: 2.0s  (base * 2)
Attempt 3: 3.0s  (base * 3)
Attempt 4: 4.0s  (base * 4)
Attempt 5: 5.0s  (base * 5)
```

**Jitter** (10% random variance):
```
Without jitter: [1.0s, 2.0s, 4.0s, 8.0s]
With jitter:    [1.05s, 2.08s, 3.92s, 8.13s]
```

#### Best Practices

**DO**:
- Use header-based retry for APIs that provide `Retry-After`
- Enable jitter to prevent thundering herd
- Set reasonable max_retries (3-5 for most APIs)
- Increase base_backoff for strict APIs

**DON'T**:
- Retry forever (always have max_retries)
- Use fixed backoff without good reason
- Ignore Retry-After headers
- Retry non-retryable errors

---

### Logging Philosophy

#### Principles

1. **Multi-Layer Logging**: Each layer logs its concerns
2. **Structured Logs**: Consistent format across all modules
3. **Emoji Icons**: Visual parsing for log levels (🚀 ✅ ⚠️ 🛑)
4. **Context Preservation**: Include URL, status code, retry count
5. **Noise Reduction**: Silence verbose libraries (`httpx`, `urllib3`)

#### Log Levels

| Level | Icon | Usage | Example |
|-------|------|-------|---------|
| `DEBUG` | 🌐 | HTTP requests | `🌐 [SyncHttpClient] GET https://api.brevo.com/v3/campaigns` |
| `INFO` | 🚀 ✅ 📊 | Task start/success/stats | `🚀 Starting Newsletter ETL for audience: native` |
| `WARNING` | ⚠️ | Retries, partial failures | `⚠️ Retry 2/5, waiting 4.2s` |
| `ERROR` | 🛑 ❌ | Terminal errors | `🛑 Circuit breaker is open for Brevo API` |
| `CRITICAL` | ❌ | Job-level failures | `❌ ETL Process Failed: RateLimitError` |

#### Layer-Specific Logging

**HTTP Clients** (`http_tools/clients.py`):
```python
logger.debug(f"🌐 [SyncHttpClient] GET {url}")
logger.warning(f"⚠️ [SyncHttpClient] Retry {attempt}/{max_retries}, waiting {wait:.2f}s")
logger.error(f"🛑 [SyncHttpClient] HTTP 429: Rate limit exceeded | URL: {url}")
```

**Connectors** (`connectors/brevo/client.py`):
```python
logger.info(f"Fetching email campaigns with limit={limit}, offset={offset}")
logger.debug(f"Response: {response.status_code}, campaigns={len(campaigns)}")
```

**Tasks** (`jobs/brevo/tasks.py`):
```python
logger.info(f"🚀 [Sync] Starting Newsletter ETL for audience: {audience}")
logger.error(f"🛑 Circuit breaker is open for Brevo API. Job will retry later.")
logger.warning(f"⚠️ Failed to fetch data for {len(failed_templates)} templates")
logger.info(f"📊 Processed {success}/{total} templates successfully")
logger.info(f"✅ Newsletter ETL finished.")
```

**Main** (`jobs/brevo/main.py`):
```python
logger.info(f"🚀 Starting Brevo ETL | Target: {target} | Audience: {audience}")
logger.info(f"✅ ETL {target} for {audience} completed successfully.")
logger.critical(f"❌ ETL Process Failed: {e}", exc_info=True)
```

#### Configuration

```python
# Silence noisy libraries
for name in ("httpx", "httpcore", "urllib3", "google"):
    logging.getLogger(name).setLevel(logging.WARNING)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

# Per-client log level control
ENV_LOG_LEVEL = os.getenv("CLIENT_LOG_LEVEL", "INFO")
CLIENT_LOG_LEVEL = getattr(logging, ENV_LOG_LEVEL, logging.INFO)
```

#### Best Practices

**DO**:
- Log task start/success/failure at INFO level
- Log retry attempts at WARNING level
- Log errors with context (URL, status code, error message)
- Use emojis for visual parsing
- Include summary statistics (success/failure counts)

**DON'T**:
- Log sensitive data (API keys, tokens, PII)
- Log at DEBUG in production (too verbose)
- Log the same error multiple times
- Use print() instead of logger

---

## Testing

---

## Implementation Guide: Adding New APIs

Follow these steps to integrate a new API (e.g., SendGrid, Mailchimp, Stripe).

### Step 1: Create Connector Structure

```bash
mkdir -p connectors/sendgrid
touch connectors/sendgrid/__init__.py
touch connectors/sendgrid/client.py
touch connectors/sendgrid/auth.py
touch connectors/sendgrid/limiter.py
touch connectors/sendgrid/config.py
```

### Step 2: Implement Configuration

**`connectors/sendgrid/config.py`**:

```python
import os
from google.cloud import secretmanager

def get_sendgrid_api_key() -> str:
    """Fetch SendGrid API key from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secret_name = "projects/PROJECT_ID/secrets/sendgrid-api-key/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8")

# API Configuration
SENDGRID_BASE_URL = "https://api.sendgrid.com/v3"
SENDGRID_API_KEY = get_sendgrid_api_key()
```

### Step 3: Implement Auth Manager

**`connectors/sendgrid/auth.py`**:

```python
from http_tools.auth import BaseAuthManager

class SendGridAuthManager(BaseAuthManager):
    """SendGrid uses API Key in Authorization header."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_token(self, force: bool = False) -> str:
        """SendGrid doesn't use refresh tokens."""
        return self.api_key

    async def get_atoken(self, force: bool = False) -> str:
        return self.api_key

    def get_headers(self, token: str) -> dict:
        return {"Authorization": f"Bearer {token}"}
```

### Step 4: Implement Rate Limiter

**`connectors/sendgrid/limiter.py`**:

```python
import time
from http_tools.rate_limiters import BaseRateLimiter

class SendGridRateLimiter(BaseRateLimiter):
    """
    SendGrid rate limits:
    - 600 requests per minute (10 per second)
    - Uses X-RateLimit-* headers
    """

    def __init__(self):
        self.max_requests_per_minute = 600
        self.window_start = time.time()
        self.request_count = 0

    def acquire(self):
        """Proactive rate limiting."""
        now = time.time()

        # Reset window if 60 seconds passed
        if now - self.window_start >= 60.0:
            self.window_start = now
            self.request_count = 0

        # If at limit, wait for window reset
        if self.request_count >= self.max_requests_per_minute:
            wait_time = 60.0 - (now - self.window_start)
            if wait_time > 0:
                time.sleep(wait_time)
            self.window_start = time.time()
            self.request_count = 0

        self.request_count += 1

    def backoff(self, response):
        """Reactive rate limiting based on headers."""
        reset_header = response.headers.get("X-RateLimit-Reset")
        if reset_header:
            reset_time = int(reset_header)
            wait = max(0, reset_time - time.time())
            time.sleep(wait)
```

### Step 5: Implement Client

**`connectors/sendgrid/client.py`**:

```python
from http_tools.clients import BaseHttpClient

class SendGridConnector:
    """SendGrid API Connector."""

    BASE_URL = "https://api.sendgrid.com/v3"

    def __init__(self, client: BaseHttpClient):
        self.client = client

    def get_campaigns(self, limit: int = 100, offset: int = 0):
        """Fetch email campaigns."""
        url = f"{self.BASE_URL}/marketing/campaigns"
        params = {"limit": limit, "offset": offset}
        return self.client.request("GET", url, params=params)

    def get_stats(self, campaign_id: str, start_date: str, end_date: str):
        """Fetch campaign statistics."""
        url = f"{self.BASE_URL}/marketing/stats/campaigns/{campaign_id}"
        params = {"start_date": start_date, "end_date": end_date}
        return self.client.request("GET", url, params=params)
```

### Step 6: Create Factory

**`factories/sendgrid.py`**:

```python
from connectors.sendgrid.auth import SendGridAuthManager
from connectors.sendgrid.client import SendGridConnector
from connectors.sendgrid.config import SENDGRID_API_KEY
from connectors.sendgrid.limiter import SendGridRateLimiter
from http_tools.circuit_breakers import CircuitBreaker, CircuitBreakerConfig
from http_tools.clients import AsyncHttpClient, SyncHttpClient
from http_tools.retry_strategies import RetryPolicy, create_retry_strategy

class SendGridFactory:
    """Factory for creating SendGrid connectors with strategies."""

    @staticmethod
    def create_connector(
        is_async: bool = False,
        use_enhanced_retry: bool = True,
        use_circuit_breaker: bool = True,
    ) -> SendGridConnector:
        """
        Create a fully-configured SendGrid connector.

        Args:
            is_async: Use async HTTP client
            use_enhanced_retry: Enable retry strategies
            use_circuit_breaker: Enable circuit breaker

        Returns:
            Configured SendGridConnector instance
        """
        # 1. Auth manager
        auth = SendGridAuthManager(api_key=SENDGRID_API_KEY)

        # 2. Rate limiter
        limiter = SendGridRateLimiter()

        # 3. Retry strategy (header-based for 429, exponential for 5xx)
        retry_strategy = None
        if use_enhanced_retry:
            policy = RetryPolicy(
                max_retries=5,
                backoff_strategy="header",
                base_backoff_seconds=10.0,
                jitter=True,
                retryable_status_codes=[429, 502, 503, 504],
            )
            retry_strategy = create_retry_strategy(policy)

        # 4. Circuit breaker
        circuit_breaker = None
        if use_circuit_breaker:
            config = CircuitBreakerConfig(
                failure_threshold=10,
                timeout_seconds=120,
                success_threshold=2,
            )
            circuit_breaker = CircuitBreaker(config)

        # 5. HTTP client
        if is_async:
            client = AsyncHttpClient(
                rate_limiter=limiter,
                auth_manager=auth,
                retry_strategy=retry_strategy,
                circuit_breaker=circuit_breaker,
            )
        else:
            client = SyncHttpClient(
                rate_limiter=limiter,
                auth_manager=auth,
                retry_strategy=retry_strategy,
                circuit_breaker=circuit_breaker,
            )

        # 6. Return connector
        return SendGridConnector(client)
```

### Step 7: Create Job Structure

```bash
mkdir -p jobs/sendgrid
touch jobs/sendgrid/__init__.py
touch jobs/sendgrid/main.py
touch jobs/sendgrid/tasks.py
touch jobs/sendgrid/transform.py
touch jobs/sendgrid/load.py
touch jobs/sendgrid/config.py
```

### Step 8: Implement Tasks with Error Handling

**`jobs/sendgrid/tasks.py`**:

```python
import logging
from datetime import datetime

from http_tools.circuit_breakers import CircuitBreakerOpenError
from http_tools.exceptions import HttpClientError, RateLimitError, ServerError

logger = logging.getLogger(__name__)

def run_campaigns_etl(connector, start_date: datetime, end_date: datetime):
    """ETL for SendGrid campaigns with proper error handling."""
    logger.info("🚀 Starting SendGrid Campaigns ETL")

    # Extract
    try:
        resp = connector.get_campaigns(limit=100)

    except CircuitBreakerOpenError:
        logger.error("🛑 Circuit breaker is open for SendGrid API")
        raise

    except RateLimitError as e:
        logger.error(f"🛑 Rate limit exceeded. Retry-After: {e.retry_after}s")
        raise

    except ServerError as e:
        logger.error(f"🛑 Server error (5xx). Status: {e.status_code}")
        raise

    except HttpClientError as e:
        logger.error(f"🛑 HTTP error: {e}")
        raise

    campaigns = resp.json().get("result", [])
    logger.info(f"Fetched {len(campaigns)} campaigns")

    if not campaigns:
        logger.info("No campaigns found")
        return

    # Transform
    from jobs.sendgrid.transform import transform_campaigns
    df = transform_campaigns(campaigns)

    # Load
    from jobs.sendgrid.load import save_to_bigquery
    save_to_bigquery(df, table_name="sendgrid_campaigns")

    logger.info("✅ SendGrid Campaigns ETL finished")
```

### Step 9: Implement Main Entry Point

**`jobs/sendgrid/main.py`**:

```python
import logging
from datetime import datetime

import typer

from factories.sendgrid import SendGridFactory
from jobs.sendgrid.tasks import run_campaigns_etl

# Silence noisy libraries
for name in ("httpx", "httpcore", "urllib3", "google"):
    logging.getLogger(name).setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="SendGrid ETL Pipeline")

@app.command()
def run(
    start_date: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(..., help="End date (YYYY-MM-DD)"),
):
    """Run SendGrid ETL."""
    logger.info(f"🚀 Starting SendGrid ETL | {start_date} to {end_date}")

    # Parse dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Create connector
    connector = SendGridFactory.create_connector()

    # Run ETL
    try:
        run_campaigns_etl(connector, start_dt, end_dt)
        logger.info("✅ SendGrid ETL completed successfully")
    except Exception as e:
        logger.critical(f"❌ ETL Process Failed: {e}", exc_info=True)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
```

### Step 10: Test Integration

**Unit Tests** (`tests/test_sendgrid_connector.py`):

```python
import pytest
from unittest.mock import Mock, patch

from connectors.sendgrid.client import SendGridConnector
from http_tools.exceptions import RateLimitError

def test_get_campaigns():
    """Test successful campaigns fetch."""
    mock_client = Mock()
    mock_response = Mock()
    mock_response.json.return_value = {"result": [{"id": 1, "name": "Campaign 1"}]}
    mock_client.request.return_value = mock_response

    connector = SendGridConnector(mock_client)
    response = connector.get_campaigns(limit=10)

    assert response.json()["result"][0]["name"] == "Campaign 1"
    mock_client.request.assert_called_once()

def test_rate_limit_error():
    """Test rate limit handling."""
    mock_client = Mock()
    mock_client.request.side_effect = RateLimitError(
        "Rate limit exceeded",
        "https://api.sendgrid.com/v3/campaigns",
        retry_after=60
    )

    connector = SendGridConnector(mock_client)

    with pytest.raises(RateLimitError) as exc_info:
        connector.get_campaigns()

    assert exc_info.value.retry_after == 60
```

**Integration Test** (`tests/test_sendgrid_factory.py`):

```python
from factories.sendgrid import SendGridFactory

def test_factory_creates_connector():
    """Test factory creates properly configured connector."""
    connector = SendGridFactory.create_connector(
        use_enhanced_retry=True,
        use_circuit_breaker=True,
    )

    assert connector is not None
    assert connector.client.retry_strategy is not None
    assert connector.client.circuit_breaker is not None
```

---

## Examples

### Example 1: Basic Connector Usage

```python
from factories.brevo import BrevoFactory

# Create connector (uses enhanced retry + circuit breaker by default)
connector = BrevoFactory.create_connector(audience="native")

# Fetch campaigns
response = connector.get_email_campaigns()
campaigns = response.json().get("campaigns", [])

print(f"Fetched {len(campaigns)} campaigns")
```

### Example 2: Custom Retry Policy

```python
from factories.brevo import BrevoFactory
from http_tools.retry_strategies import RetryPolicy, create_retry_strategy
from http_tools.clients import SyncHttpClient
from connectors.brevo.auth import BrevoAuthManager
from connectors.brevo.limiter import SyncBrevoRateLimiter

# Custom aggressive retry
policy = RetryPolicy(
    max_retries=10,
    backoff_strategy="exponential",
    base_backoff_seconds=0.5,
    jitter=True,
)
retry_strategy = create_retry_strategy(policy)

# Build manually
auth = BrevoAuthManager(api_key="your-key")
limiter = SyncBrevoRateLimiter()
client = SyncHttpClient(
    rate_limiter=limiter,
    auth_manager=auth,
    retry_strategy=retry_strategy,
)

from connectors.brevo.client import BrevoConnector
connector = BrevoConnector(client)
```

### Example 3: Async with Graceful Degradation

```python
import asyncio
from factories.brevo import BrevoFactory
from http_tools.exceptions import HttpClientError
from http_tools.circuit_breakers import CircuitBreakerOpenError

async def fetch_all_templates(connector, template_ids):
    """Fetch templates with graceful degradation."""

    async def fetch_one(template_id):
        try:
            return await connector.get_template(template_id)
        except (CircuitBreakerOpenError, HttpClientError) as e:
            logger.error(f"Failed to fetch template {template_id}: {e}")
            return None

    # Fetch all in parallel
    results = await asyncio.gather(*[fetch_one(t_id) for t_id in template_ids])

    # Filter out failures
    successful = [r for r in results if r is not None]
    failed_count = len(template_ids) - len(successful)

    logger.info(f"Fetched {len(successful)}/{len(template_ids)} templates")

    return successful

# Usage
connector = BrevoFactory.create_connector(is_async=True)
results = asyncio.run(fetch_all_templates(connector, [1, 2, 3, 4, 5]))
```

---

## Testing Strategy


## Overview

This guide provides a comprehensive testing strategy for the 4-layer factory pattern architecture used in this codebase.

## Testing Pyramid

```
                    ┌─────────────┐
                    │   E2E (5%)  │  Full ETL workflows
                    └─────────────┘
                ┌───────────────────┐
                │ Integration (15%) │  Layer interactions, real APIs
                └───────────────────┘
            ┌───────────────────────────┐
            │   Contract Tests (20%)    │  Interface compliance
            └───────────────────────────┘
        ┌───────────────────────────────────┐
        │        Unit Tests (60%)           │  Individual components
        └───────────────────────────────────┘
```

## Directory Structure

```
tests/
├── unit/
│   ├── http_tools/              # Layer 1: HTTP Tools
│   │   ├── test_auth.py
│   │   ├── test_rate_limiters.py
│   │   ├── test_retry_strategies.py
│   │   ├── test_circuit_breakers.py
│   │   └── test_clients.py
│   ├── connectors/              # Layer 2: Connectors
│   │   ├── brevo/
│   │   │   ├── test_auth.py
│   │   │   ├── test_client.py
│   │   │   ├── test_limiter.py
│   │   │   └── test_config.py
│   │   └── titelive/
│   │       ├── test_auth.py
│   │       ├── test_client.py
│   │       ├── test_limiter.py
│   │       └── test_config.py
│   ├── factories/               # Layer 3: Factories
│   │   ├── test_brevo_factory.py
│   │   └── test_titelive_factory.py
│   └── jobs/                    # Layer 4: Jobs
│       ├── brevo/
│       │   ├── test_tasks.py
│       │   └── test_transform.py
│       └── titelive/
│           ├── test_tasks.py
│           └── test_transform.py
├── integration/
│   ├── test_brevo_integration.py
│   ├── test_titelive_integration.py
│   └── test_factory_assembly.py
├── contract/
│   ├── test_auth_contracts.py
│   ├── test_connector_contracts.py
│   └── test_factory_contracts.py
├── e2e/
│   ├── test_brevo_etl.py
│   └── test_titelive_etl.py
├── fixtures/
│   ├── __init__.py
│   ├── api_responses.py        # Mock API responses
│   ├── connectors.py            # Connector fixtures
│   └── factories.py             # Factory fixtures
└── conftest.py                  # Shared pytest configuration
```

---

## Layer 1: HTTP Tools (Foundation) - Unit Tests

### Test Strategy
- **Pure unit tests**: Mock all external dependencies
- **Focus**: Test behavior, not implementation
- **Coverage target**: 90%+

### Example: `tests/unit/http_tools/test_rate_limiters.py`

```python
"""Unit tests for rate limiters."""

import time
import pytest
from unittest.mock import Mock, patch

from http_tools.rate_limiters import (
    SyncTokenBucketRateLimiter,
    BaseRateLimiter,
)


class TestSyncTokenBucketRateLimiter:
    """Test token bucket rate limiter."""

    def test_enforces_rate_limit(self):
        """Test that rate limiter enforces configured rate."""
        limiter = SyncTokenBucketRateLimiter(calls=2, period=1.0)

        start = time.time()
        limiter.acquire()  # 0s
        limiter.acquire()  # 0s (2 calls allowed in 1s)
        limiter.acquire()  # Should wait ~1s for refill
        elapsed = time.time() - start

        # Should take ~1 second (with tolerance)
        assert 0.9 <= elapsed <= 1.2

    def test_allows_burst_within_period(self):
        """Test burst of requests within period."""
        limiter = SyncTokenBucketRateLimiter(calls=5, period=1.0)

        start = time.time()
        for _ in range(5):
            limiter.acquire()
        elapsed = time.time() - start

        # All 5 should complete immediately
        assert elapsed < 0.1

    def test_refills_tokens_over_time(self):
        """Test token bucket refills over time."""
        limiter = SyncTokenBucketRateLimiter(calls=2, period=1.0)

        # Use up all tokens
        limiter.acquire()
        limiter.acquire()

        # Wait for refill
        time.sleep(1.1)

        # Should have tokens again
        start = time.time()
        limiter.acquire()
        limiter.acquire()
        elapsed = time.time() - start

        # Should complete immediately
        assert elapsed < 0.1

    def test_backoff_method_exists(self):
        """Test backoff method is implemented."""
        limiter = SyncTokenBucketRateLimiter(calls=1, period=1.0)
        mock_response = Mock()

        # Should not raise
        limiter.backoff(mock_response)


class TestBaseRateLimiter:
    """Test base rate limiter interface."""

    def test_is_abstract(self):
        """Test BaseRateLimiter cannot be instantiated."""
        with pytest.raises(TypeError):
            BaseRateLimiter()

    def test_subclass_must_implement_acquire(self):
        """Test subclass must implement acquire."""
        class IncompleteRateLimiter(BaseRateLimiter):
            def backoff(self, response):
                pass

        with pytest.raises(TypeError):
            IncompleteRateLimiter()

    def test_subclass_must_implement_backoff(self):
        """Test subclass must implement backoff."""
        class IncompleteRateLimiter(BaseRateLimiter):
            def acquire(self):
                pass

        with pytest.raises(TypeError):
            IncompleteRateLimiter()


# Parametrized tests for different configurations
@pytest.mark.parametrize("calls,period,expected_rate", [
    (1, 1.0, 1.0),
    (10, 1.0, 10.0),
    (1, 0.1, 10.0),
    (100, 10.0, 10.0),
])
def test_rate_limiter_configurations(calls, period, expected_rate):
    """Test various rate limiter configurations."""
    limiter = SyncTokenBucketRateLimiter(calls=calls, period=period)

    # Measure actual rate
    iterations = min(calls * 3, 30)  # Limit test duration
    start = time.time()
    for _ in range(iterations):
        limiter.acquire()
    elapsed = time.time() - start

    actual_rate = iterations / elapsed

    # Allow 20% tolerance for timing variations
    assert expected_rate * 0.8 <= actual_rate <= expected_rate * 1.2
```

### Example: `tests/unit/http_tools/test_circuit_breakers.py`

```python
"""Unit tests for circuit breakers."""

import time
import pytest
from unittest.mock import Mock

from http_tools.circuit_breakers import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
)


class TestCircuitBreaker:
    """Test circuit breaker state machine."""

    def test_starts_in_closed_state(self):
        """Test circuit breaker starts closed."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config)

        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_failures(self):
        """Test circuit opens after failure threshold."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config)

        def failing_func():
            raise ValueError("Test error")

        # Should fail 3 times, then open
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(failing_func)

        assert cb.state == CircuitState.OPEN

    def test_blocks_requests_when_open(self):
        """Test circuit breaker blocks requests when open."""
        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker(config)

        # Trigger failure to open circuit
        with pytest.raises(ValueError):
            cb.call(lambda: 1 / 0)

        # Next call should be blocked
        with pytest.raises(CircuitBreakerOpenError) as exc_info:
            cb.call(lambda: "success")

        assert "Circuit breaker is OPEN" in str(exc_info.value)

    def test_transitions_to_half_open_after_timeout(self):
        """Test circuit transitions to half-open after timeout."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=0.1  # Short timeout for test
        )
        cb = CircuitBreaker(config)

        # Open circuit
        with pytest.raises(ValueError):
            cb.call(lambda: 1 / 0)

        assert cb.state == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.15)

        # Should transition to half-open on next attempt
        result = cb.call(lambda: "success")

        assert cb.state == CircuitState.HALF_OPEN
        assert result == "success"

    def test_closes_after_success_threshold_in_half_open(self):
        """Test circuit closes after success threshold in half-open."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=0.1,
            success_threshold=2
        )
        cb = CircuitBreaker(config)

        # Open circuit
        with pytest.raises(ValueError):
            cb.call(lambda: 1 / 0)

        # Wait and enter half-open
        time.sleep(0.15)

        # Make 2 successful calls
        cb.call(lambda: "success")
        cb.call(lambda: "success")

        # Should be closed now
        assert cb.state == CircuitState.CLOSED

    def test_reopens_on_failure_in_half_open(self):
        """Test circuit reopens on failure in half-open."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=0.1
        )
        cb = CircuitBreaker(config)

        # Open circuit
        with pytest.raises(ValueError):
            cb.call(lambda: 1 / 0)

        # Wait and enter half-open
        time.sleep(0.15)
        cb.call(lambda: "success")  # Enter half-open

        # Fail in half-open
        with pytest.raises(ValueError):
            cb.call(lambda: 1 / 0)

        # Should reopen
        assert cb.state == CircuitState.OPEN

    def test_resets_failure_count_on_success(self):
        """Test failure count resets on success."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config)

        # 2 failures
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(lambda: 1 / 0)

        # 1 success - should reset count
        cb.call(lambda: "success")

        # 2 more failures - should not open (count reset)
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(lambda: 1 / 0)

        assert cb.state == CircuitState.CLOSED
```

---

## Layer 2: Connectors - Unit Tests

### Test Strategy
- **Mock HTTP client**: Test connector logic without network calls
- **Focus**: API route implementations, parameter handling, error propagation
- **Coverage target**: 85%+

### Example: `tests/unit/connectors/titelive/test_client.py`

```python
"""Unit tests for Titelive connector client."""

import pytest
from unittest.mock import Mock, MagicMock

from connectors.titelive.client import TiteliveConnector
from http_tools.exceptions import HttpClientError, RateLimitError


class TestTiteliveConnector:
    """Test Titelive connector."""

    @pytest.fixture
    def mock_client(self):
        """Create mock HTTP client."""
        client = Mock()
        return client

    @pytest.fixture
    def connector(self, mock_client):
        """Create connector with mock client."""
        return TiteliveConnector(client=mock_client)

    def test_get_by_eans_success(self, connector, mock_client):
        """Test successful EAN fetch."""
        # Arrange
        mock_response = Mock()
        mock_response.json.return_value = {
            "result": [{"ean": "123", "title": "Book"}]
        }
        mock_client.request.return_value = mock_response

        # Act
        result = connector.get_by_eans(["123", "456"])

        # Assert
        mock_client.request.assert_called_once_with(
            "GET",
            "https://catsearch.epagine.fr/v1/ean",
            params={"in": "ean=123|456"}
        )
        assert result == {"result": [{"ean": "123", "title": "Book"}]}

    def test_get_by_eans_empty_list_raises_error(self, connector):
        """Test empty EAN list raises ValueError."""
        with pytest.raises(ValueError, match="EAN list cannot be empty"):
            connector.get_by_eans([])

    def test_get_by_eans_exceeds_limit_raises_error(self, connector):
        """Test EAN list exceeding 250 raises ValueError."""
        eans = [str(i) for i in range(251)]

        with pytest.raises(ValueError, match="exceeds API limit"):
            connector.get_by_eans(eans)

    def test_get_by_eans_with_base(self, connector, mock_client):
        """Test EAN fetch with base parameter."""
        mock_response = Mock()
        mock_response.json.return_value = {"result": []}
        mock_client.request.return_value = mock_response

        connector.get_by_eans_with_base(["123"], base="paper")

        mock_client.request.assert_called_once()
        call_args = mock_client.request.call_args
        assert call_args[1]["params"]["base"] == "paper"

    def test_search_by_date_required_params(self, connector, mock_client):
        """Test search by date with required parameters."""
        mock_response = Mock()
        mock_response.json.return_value = {"result": [], "nbresults": 0}
        mock_client.request.return_value = mock_response

        connector.search_by_date(
            base="paper",
            min_date="01/01/2024",
            page=1,
            results_per_page=120
        )

        mock_client.request.assert_called_once()
        call_args = mock_client.request.call_args
        params = call_args[1]["params"]

        assert params["base"] == "paper"
        assert params["dateminm"] == "01/01/2024"
        assert params["page"] == "1"
        assert params["nombre"] == "120"

    def test_search_by_date_with_max_date(self, connector, mock_client):
        """Test search by date with max_date parameter."""
        mock_response = Mock()
        mock_response.json.return_value = {"result": []}
        mock_client.request.return_value = mock_response

        connector.search_by_date(
            base="music",
            min_date="01/01/2024",
            max_date="31/01/2024"
        )

        call_args = mock_client.request.call_args
        params = call_args[1]["params"]
        assert params["datemaxm"] == "31/01/2024"

    def test_propagates_http_errors(self, connector, mock_client):
        """Test connector propagates HTTP errors from client."""
        mock_client.request.side_effect = RateLimitError(
            "Rate limited",
            "https://api.example.com",
            retry_after=60
        )

        with pytest.raises(RateLimitError):
            connector.get_by_eans(["123"])


class TestTiteliveConnectorAuth:
    """Test Titelive auth manager."""

    def test_auto_refresh_logic(self):
        """Test token auto-refresh logic."""
        from connectors.titelive.auth import TiteliveAuthManager
        from unittest.mock import patch
        import time

        with patch('connectors.titelive.auth.get_titelive_credentials') as mock_creds:
            mock_creds.return_value = ("user", "pass")

            with patch('connectors.titelive.auth.requests.post') as mock_post:
                mock_post.return_value.json.return_value = {"token": "abc123"}
                mock_post.return_value.status_code = 200

                auth = TiteliveAuthManager(project_id="test")
                auth.token_lifetime = 10  # 10 seconds for test
                auth.refresh_buffer = 2   # Refresh at 8 seconds

                # Get initial token
                token1 = auth.get_token()
                assert token1 == "abc123"

                # Simulate time passage (past refresh buffer)
                auth._token_timestamp = time.time() - 9  # 9 seconds ago

                # Should trigger refresh
                mock_post.return_value.json.return_value = {"token": "xyz789"}
                token2 = auth.get_token()
                assert token2 == "xyz789"
                assert mock_post.call_count == 2
```

---

## Layer 3: Factories - Unit Tests

### Test Strategy
- **Mock all dependencies**: Connectors, auth, rate limiters
- **Focus**: Dependency injection, strategy selection, configuration
- **Coverage target**: 95%+

### Example: `tests/unit/factories/test_titelive_factory.py`

```python
"""Unit tests for Titelive factory."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from factories.titelive import TiteliveFactory
from connectors.titelive.limiter import (
    TiteliveBasicRateLimiter,
    TiteliveBurstRecoveryLimiter,
)
from http_tools.circuit_breakers import CircuitBreaker


class TestTiteliveFactory:
    """Test Titelive factory connector creation."""

    @patch('factories.titelive.TiteliveConnector')
    @patch('factories.titelive.SyncHttpClient')
    @patch('factories.titelive.TiteliveAuthManager')
    def test_creates_connector_with_defaults(
        self, mock_auth, mock_client, mock_connector
    ):
        """Test factory creates connector with default configuration."""
        # Arrange
        mock_auth_instance = Mock()
        mock_auth.return_value = mock_auth_instance

        mock_client_instance = Mock()
        mock_client.return_value = mock_client_instance

        mock_connector_instance = Mock()
        mock_connector.return_value = mock_connector_instance

        # Act
        result = TiteliveFactory.create_connector(project_id="test-project")

        # Assert
        mock_auth.assert_called_once_with(project_id="test-project")
        mock_client.assert_called_once()
        mock_connector.assert_called_once_with(client=mock_client_instance)
        assert result == mock_connector_instance

    @patch('factories.titelive.TiteliveConnector')
    @patch('factories.titelive.SyncHttpClient')
    @patch('factories.titelive.TiteliveAuthManager')
    @patch('factories.titelive.TiteliveBasicRateLimiter')
    def test_uses_basic_limiter_by_default(
        self, mock_limiter, mock_auth, mock_client, mock_connector
    ):
        """Test factory uses basic rate limiter by default."""
        TiteliveFactory.create_connector(
            project_id="test-project",
            use_burst_recovery=False
        )

        mock_limiter.assert_called_once()

    @patch('factories.titelive.TiteliveConnector')
    @patch('factories.titelive.SyncHttpClient')
    @patch('factories.titelive.TiteliveAuthManager')
    @patch('factories.titelive.TiteliveBurstRecoveryLimiter')
    def test_uses_burst_limiter_when_enabled(
        self, mock_limiter, mock_auth, mock_client, mock_connector
    ):
        """Test factory uses burst-recovery limiter when enabled."""
        TiteliveFactory.create_connector(
            project_id="test-project",
            use_burst_recovery=True
        )

        mock_limiter.assert_called_once()

    @patch('factories.titelive.TiteliveConnector')
    @patch('factories.titelive.SyncHttpClient')
    @patch('factories.titelive.TiteliveAuthManager')
    @patch('factories.titelive.create_retry_strategy')
    def test_configures_retry_strategy_when_enabled(
        self, mock_retry, mock_auth, mock_client, mock_connector
    ):
        """Test factory configures retry strategy when enabled."""
        TiteliveFactory.create_connector(
            project_id="test-project",
            use_enhanced_retry=True
        )

        mock_retry.assert_called_once()
        # Verify retry policy
        call_args = mock_retry.call_args[0][0]
        assert call_args.max_retries == 3
        assert call_args.backoff_strategy == "exponential"

    @patch('factories.titelive.TiteliveConnector')
    @patch('factories.titelive.SyncHttpClient')
    @patch('factories.titelive.TiteliveAuthManager')
    def test_disables_retry_when_disabled(
        self, mock_auth, mock_client, mock_connector
    ):
        """Test factory passes None for retry strategy when disabled."""
        TiteliveFactory.create_connector(
            project_id="test-project",
            use_enhanced_retry=False
        )

        # Verify retry_strategy=None passed to client
        call_kwargs = mock_client.call_args[1]
        assert call_kwargs.get('retry_strategy') is None

    @patch('factories.titelive.TiteliveConnector')
    @patch('factories.titelive.SyncHttpClient')
    @patch('factories.titelive.TiteliveAuthManager')
    @patch('factories.titelive.CircuitBreaker')
    def test_configures_circuit_breaker_when_enabled(
        self, mock_cb, mock_auth, mock_client, mock_connector
    ):
        """Test factory configures circuit breaker when enabled."""
        TiteliveFactory.create_connector(
            project_id="test-project",
            use_circuit_breaker=True
        )

        mock_cb.assert_called_once()
        # Verify circuit breaker config
        call_args = mock_cb.call_args[0][0]
        assert call_args.failure_threshold == 5
        assert call_args.timeout_seconds == 30

    @patch('factories.titelive.TiteliveConnector')
    @patch('factories.titelive.SyncHttpClient')
    @patch('factories.titelive.TiteliveAuthManager')
    def test_passes_all_components_to_client(
        self, mock_auth, mock_client, mock_connector
    ):
        """Test factory passes all components to HTTP client."""
        TiteliveFactory.create_connector(
            project_id="test-project",
            use_burst_recovery=True,
            use_enhanced_retry=True,
            use_circuit_breaker=True
        )

        # Verify all components passed to client
        call_kwargs = mock_client.call_args[1]
        assert 'rate_limiter' in call_kwargs
        assert 'auth_manager' in call_kwargs
        assert 'retry_strategy' in call_kwargs
        assert 'circuit_breaker' in call_kwargs
```

---

## Integration Tests

### Test Strategy
- **Test layer interactions**: Verify components work together
- **Use test doubles sparingly**: Only mock external APIs
- **Coverage target**: 70%+

### Example: `tests/integration/test_titelive_integration.py`

```python
"""Integration tests for Titelive connector."""

import pytest
import os
from unittest.mock import patch, Mock

from factories.titelive import TiteliveFactory
from http_tools.exceptions import RateLimitError


@pytest.mark.integration
class TestTiteliveIntegration:
    """Integration tests for Titelive with mocked API."""

    @pytest.fixture
    def mock_api_response(self):
        """Mock successful API response."""
        return {
            "result": [
                {"ean": "9782070612758", "title": "Test Book"}
            ]
        }

    @pytest.fixture
    def connector_with_mocked_api(self, mock_api_response):
        """Create connector with mocked API responses."""
        with patch('requests.post') as mock_post:
            # Mock token fetch
            mock_post.return_value.json.return_value = {"token": "test-token"}
            mock_post.return_value.status_code = 200

            with patch('http_tools.clients.requests.Session') as mock_session:
                session_instance = Mock()
                mock_session.return_value = session_instance

                # Mock API call
                response = Mock()
                response.json.return_value = mock_api_response
                response.status_code = 200
                session_instance.request.return_value = response

                connector = TiteliveFactory.create_connector(
                    project_id="test-project",
                    use_burst_recovery=False,
                    use_enhanced_retry=True,
                    use_circuit_breaker=True,
                )

                yield connector

    def test_factory_creates_working_connector(self, connector_with_mocked_api):
        """Test factory creates functional connector."""
        result = connector_with_mocked_api.get_by_eans(["9782070612758"])

        assert result is not None
        assert "result" in result
        assert len(result["result"]) == 1

    def test_rate_limiter_integrated(self):
        """Test rate limiter works with real connector."""
        import time

        with patch('requests.post') as mock_post:
            mock_post.return_value.json.return_value = {"token": "test-token"}
            mock_post.return_value.status_code = 200

            with patch('http_tools.clients.requests.Session') as mock_session:
                session_instance = Mock()
                mock_session.return_value = session_instance

                response = Mock()
                response.json.return_value = {"result": []}
                response.status_code = 200
                session_instance.request.return_value = response

                connector = TiteliveFactory.create_connector(
                    project_id="test-project",
                    use_burst_recovery=False  # 1 req/sec
                )

                # Make 3 requests, should take ~2 seconds
                start = time.time()
                for _ in range(3):
                    connector.get_by_eans(["123"])
                elapsed = time.time() - start

                assert 1.8 <= elapsed <= 2.5

    def test_retry_strategy_integrated(self):
        """Test retry strategy works with connector."""
        with patch('requests.post') as mock_post:
            mock_post.return_value.json.return_value = {"token": "test-token"}
            mock_post.return_value.status_code = 200

            with patch('http_tools.clients.requests.Session') as mock_session:
                session_instance = Mock()
                mock_session.return_value = session_instance

                # First call fails, second succeeds
                response_fail = Mock()
                response_fail.status_code = 503
                response_fail.raise_for_status.side_effect = Exception("Server error")

                response_success = Mock()
                response_success.json.return_value = {"result": []}
                response_success.status_code = 200

                session_instance.request.side_effect = [
                    response_fail,
                    response_success
                ]

                connector = TiteliveFactory.create_connector(
                    project_id="test-project",
                    use_enhanced_retry=True
                )

                # Should succeed after retry
                result = connector.get_by_eans(["123"])
                assert result is not None
                assert session_instance.request.call_count == 2
```

---

## Contract Tests

### Test Strategy
- **Verify interfaces**: Ensure contracts between layers are maintained
- **No implementation testing**: Focus on method signatures, return types
- **Coverage target**: 100% of public interfaces

### Example: `tests/contract/test_connector_contracts.py`

```python
"""Contract tests for connector interfaces."""

import pytest
from abc import ABC
import inspect

from connectors.brevo.client import BrevoConnector
from connectors.titelive.client import TiteliveConnector
from http_tools.clients import SyncHttpClient


class TestConnectorContract:
    """Verify all connectors follow the same contract."""

    @pytest.fixture
    def connectors(self):
        """Get all connector classes."""
        return [BrevoConnector, TiteliveConnector]

    def test_connectors_accept_http_client(self, connectors):
        """Test all connectors accept HTTP client in __init__."""
        for connector_class in connectors:
            sig = inspect.signature(connector_class.__init__)
            params = list(sig.parameters.keys())

            assert 'client' in params, (
                f"{connector_class.__name__} must accept 'client' parameter"
            )

            # Verify client parameter type hint
            client_param = sig.parameters['client']
            if client_param.annotation != inspect.Parameter.empty:
                # Type hint should be SyncHttpClient or compatible
                assert 'HttpClient' in str(client_param.annotation)

    def test_connectors_have_base_url(self, connectors):
        """Test all connectors define BASE_URL."""
        for connector_class in connectors:
            assert hasattr(connector_class, 'BASE_URL'), (
                f"{connector_class.__name__} must define BASE_URL"
            )
            assert isinstance(connector_class.BASE_URL, str)
            assert connector_class.BASE_URL.startswith('http')

    def test_connector_methods_return_dict(self, connectors):
        """Test connector methods return dict or raise."""
        # This is a contract: connector methods should return parsed JSON (dict)
        # or raise exceptions, never return Response objects

        for connector_class in connectors:
            methods = [
                m for m in dir(connector_class)
                if not m.startswith('_')
                and callable(getattr(connector_class, m))
                and m not in ['BASE_URL']
            ]

            for method_name in methods:
                method = getattr(connector_class, method_name)
                sig = inspect.signature(method)

                # Check return annotation if present
                if sig.return_annotation != inspect.Parameter.empty:
                    # Should return dict or dict-like
                    assert 'dict' in str(sig.return_annotation).lower(), (
                        f"{connector_class.__name__}.{method_name} "
                        f"should return dict, not {sig.return_annotation}"
                    )


class TestFactoryContract:
    """Verify all factories follow the same contract."""

    def test_factory_has_create_connector_method(self):
        """Test factory has create_connector class method."""
        from factories.brevo import BrevoFactory
        from factories.titelive import TiteliveFactory

        for factory in [BrevoFactory, TiteliveFactory]:
            assert hasattr(factory, 'create_connector')
            method = getattr(factory, 'create_connector')
            assert callable(method)

            # Should be classmethod
            assert isinstance(
                inspect.getattr_static(factory, 'create_connector'),
                classmethod
            )

    def test_factory_returns_connector(self):
        """Test factory returns connector instance."""
        from factories.titelive import TiteliveFactory
        from unittest.mock import patch, Mock

        with patch('factories.titelive.TiteliveAuthManager'):
            with patch('factories.titelive.SyncHttpClient'):
                with patch('factories.titelive.TiteliveConnector') as mock_conn:
                    mock_instance = Mock()
                    mock_conn.return_value = mock_instance

                    result = TiteliveFactory.create_connector(
                        project_id="test"
                    )

                    # Should return connector instance
                    assert result == mock_instance


class TestAuthManagerContract:
    """Verify all auth managers implement BaseAuthManager interface."""

    def test_auth_managers_implement_interface(self):
        """Test auth managers implement required methods."""
        from connectors.brevo.auth import BrevoAuthManager
        from connectors.titelive.auth import TiteliveAuthManager
        from http_tools.auth import BaseAuthManager

        for auth_class in [BrevoAuthManager, TiteliveAuthManager]:
            # Should inherit from BaseAuthManager
            assert issubclass(auth_class, BaseAuthManager)

            # Should implement required methods
            required_methods = ['get_token', 'get_atoken', 'get_headers']
            for method in required_methods:
                assert hasattr(auth_class, method), (
                    f"{auth_class.__name__} must implement {method}"
                )
```

---

## Fixtures and Test Utilities

### Example: `tests/fixtures/connectors.py`

```python
"""Reusable fixtures for connector tests."""

import pytest
from unittest.mock import Mock, MagicMock


@pytest.fixture
def mock_http_client():
    """Create mock HTTP client."""
    client = Mock()
    client.request = Mock()
    return client


@pytest.fixture
def mock_auth_manager():
    """Create mock auth manager."""
    auth = Mock()
    auth.get_token.return_value = "test-token"
    auth.get_headers.return_value = {"Authorization": "Bearer test-token"}
    return auth


@pytest.fixture
def mock_rate_limiter():
    """Create mock rate limiter."""
    limiter = Mock()
    limiter.acquire = Mock()
    limiter.backoff = Mock()
    return limiter


@pytest.fixture
def titelive_api_response():
    """Sample Titelive API response."""
    return {
        "result": [
            {
                "ean": "9782070612758",
                "titre": "Test Book",
                "auteur": "Test Author",
                "editeur": "Test Publisher",
            }
        ],
        "nbresults": 1
    }


@pytest.fixture
def brevo_api_response():
    """Sample Brevo API response."""
    return {
        "campaigns": [
            {
                "id": 1,
                "name": "Test Campaign",
                "status": "sent",
                "statistics": {
                    "globalStats": {
                        "uniqueClicks": 100,
                        "uniqueOpens": 200
                    }
                }
            }
        ],
        "count": 1
    }
```

### Example: `tests/conftest.py`

```python
"""Shared pytest configuration."""

import pytest
import logging


def pytest_configure(config):
    """Configure pytest."""
    # Add markers
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration for each test."""
    logging.getLogger().handlers = []
    logging.basicConfig(level=logging.WARNING)


@pytest.fixture
def disable_rate_limiting(monkeypatch):
    """Disable rate limiting for tests."""
    def mock_acquire(self):
        pass

    from http_tools.rate_limiters import BaseRateLimiter
    monkeypatch.setattr(BaseRateLimiter, 'acquire', mock_acquire)


@pytest.fixture
def mock_secret_manager(monkeypatch):
    """Mock GCP Secret Manager."""
    def mock_access_secret(project_id, secret_name):
        return f"fake-secret-{secret_name}"

    monkeypatch.setattr(
        'utils.gcp.access_secret_data',
        mock_access_secret
    )
```

---

## Running Tests

### Test Commands

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run specific layer
pytest tests/unit/factories/

# Run with coverage
pytest --cov=factories --cov=connectors --cov-report=html

# Run integration tests
pytest -m integration

# Run specific test file
pytest tests/unit/factories/test_titelive_factory.py

# Run with verbose output
pytest -v

# Run and show print statements
pytest -s

# Run failed tests only
pytest --lf

# Run tests in parallel
pytest -n auto
```

### pytest.ini Configuration

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

markers =
    integration: Integration tests (slower, may hit external services)
    e2e: End-to-end tests (slowest, full workflows)
    slow: Slow running tests

addopts =
    --strict-markers
    --tb=short
    --disable-warnings
    -ra

# Coverage settings
[coverage:run]
source = .
omit =
    tests/*
    */migrations/*
    */test_*.py
    setup.py

[coverage:report]
precision = 2
show_missing = True
skip_covered = False
```

---

## Best Practices

### 1. Test Organization

- **One test class per production class**
- **Descriptive test names**: `test_<what>_<condition>_<expected>`
- **Arrange-Act-Assert pattern**: Clear test structure

### 2. Mocking Strategy

```python
# ✅ GOOD: Mock at the boundary
@patch('factories.titelive.TiteliveAuthManager')
def test_factory(mock_auth):
    factory.create_connector(project_id="test")
    mock_auth.assert_called_once()

# ❌ BAD: Mock internal implementation
@patch('factories.titelive.TiteliveAuthManager._fetch_token')
def test_factory(mock_fetch):
    # Too coupled to implementation
    pass
```

### 3. Fixture Reuse

```python
# ✅ GOOD: Reusable fixtures
@pytest.fixture
def configured_connector():
    return TiteliveFactory.create_connector(
        project_id="test",
        use_burst_recovery=True
    )

def test_something(configured_connector):
    # Use fixture
    pass

# ❌ BAD: Duplicate setup
def test_something():
    connector = TiteliveFactory.create_connector(...)
    # ...

def test_another_thing():
    connector = TiteliveFactory.create_connector(...)
    # Same setup duplicated
```

### 4. Test Independence

```python
# ✅ GOOD: Independent tests
def test_a():
    limiter = RateLimiter()
    limiter.acquire()
    # Test completes

def test_b():
    limiter = RateLimiter()  # Fresh instance
    limiter.acquire()
    # Independent of test_a

# ❌ BAD: Shared state
limiter = RateLimiter()  # Module-level

def test_a():
    limiter.acquire()  # Affects limiter

def test_b():
    limiter.acquire()  # Depends on test_a state
```

---

## Summary

### Coverage Targets

| Layer | Unit Tests | Integration Tests | Total |
|-------|-----------|-------------------|-------|
| HTTP Tools | 90% | 5% | 95% |
| Connectors | 85% | 10% | 95% |
| Factories | 95% | 5% | 100% |
| Jobs | 70% | 15% | 85% |
| **Overall** | **80%** | **10%** | **90%** |

### Key Principles

1. **Test behavior, not implementation**
2. **Mock at boundaries, not internals**
3. **One assertion per test (when possible)**
4. **Fast unit tests, slower integration tests**
5. **Readable test names and structure**
6. **DRY through fixtures, not test duplication**
7. **Independent tests that can run in any order**

### Next Steps

1. Set up CI/CD pipeline to run tests automatically
2. Configure coverage reports and badges
3. Add pre-commit hooks for test execution
4. Document test writing guidelines for team
5. Create test templates for new features


---

## Additional Resources

- **Architecture Audit**: See `ETL_ARCHITECTURE_AUDIT.md` for detailed analysis
- **Refactoring Plans**: See `REFACTORING_PLAN_*.md` for design decisions
- **Implementation Summary**: See `IMPLEMENTATION_SUMMARY.md` for what was built
- **Testing Examples**: See `TESTING_EXAMPLES.md` for test patterns
- **Task Updates**: See `RECOMMENDED_TASK_UPDATES.md` for migration guide

---

## Questions?

For questions or issues, contact the data engineering team or open an issue in the repository.
