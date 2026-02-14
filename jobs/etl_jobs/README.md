# ETL Jobs Architecture

A modular, production-ready ETL framework built on the **Factory Pattern** with pluggable strategies for HTTP communication, authentication, rate limiting, error handling, retry logic, and **Pydantic-based Validation**.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Layer Design](#layer-design)
  - [Layer 0: Shared Utilities (Foundation)](#layer-0-shared-utilities-foundation)
  - [Layer 1: HTTP Tools (Foundation)](#layer-1-http-tools-foundation)
  - [Layer 2: Connectors (API-Specific)](#layer-2-connectors-api-specific)
  - [Layer 3: Factories (Assembly)](#layer-3-factories-assembly)
  - [Layer 4: Workflows (Business Logic)](#layer-4-workflows-business-logic)
- [Validation Layer](#validation-layer)
  - [Philosophy: Read vs Write Contracts](#philosophy-read-vs-write-contracts)
  - [Dynamic Schema Generation](#dynamic-schema-generation)
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
- **Configurability**: Behavior is controlled via dependency injection
- **Data Integrity**: **Validation Layer** ensures strict contracts for API responses and Database schemas

```
┌─────────────────────────────────────────────────────────┐
│  Layer 4: Workflows (ETL Business Logic)                │
│  ├─ Extract: Fetch data via connectors                  │
│  ├─ Validate: Pydantic models (Read Contract)           │
│  ├─ Transform: Map to clean models (Write Contract)     │
│  └─ Load: Auto-generate schema -> BigQuery              │
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
│  ├─ Schemas: Pydantic models for raw API responses      │
│  └─ Config: API keys, endpoints                         │
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

### Layer 0: Shared Utilities (Foundation)

**Location**: `utils/`

**Purpose**: Provide low-level, cross-cutting utilities used by all other layers.

#### Components

| Module | Purpose | Key Functions |
| :--- | :--- | :--- |
| `gcp.py` | GCP Service Interactions | `access_secret_data` (Secret Manager) |
| `schemas.py` | Schema Bridge | `pydantic_to_bigquery_schema` |

#### Intent
The `utils` layer contains logic that is not HTTP-specific (unlike Layer 1) but is needed by multiple connectors or workflows. For example, converting Pydantic models to BigQuery schemas is a shared concern that ensures our **Single Source of Truth** philosophy is maintained across all ETL jobs.

---

### Layer 1: HTTP Tools (Foundation)

**Location**: `http_tools/`

**Purpose**: Provide reusable, API-agnostic HTTP infrastructure.

#### Components

| Module                  | Purpose                   | Key Classes                                                           |
| ----------------------- | ------------------------- | --------------------------------------------------------------------- |
| `clients.py`          | HTTP request execution    | `SyncHttpClient`, `AsyncHttpClient`                               |
| `auth.py`             | Authentication strategies | `BaseAuthManager`, `APIKeyAuthManager`, `OAuth2AuthManager`     |
| `rate_limiters.py`    | Proactive rate limiting   | `BaseRateLimiter`, `TokenBucketLimiter`, `SlidingWindowLimiter` |
| `retry_strategies.py` | Retry policies            | `ExponentialBackoffRetryStrategy`, `HeaderBasedRetryStrategy`     |
| `circuit_breakers.py` | Fault tolerance           | `CircuitBreaker`, `PerEndpointCircuitBreaker`                     |
| `exceptions.py`       | Typed error hierarchy     | `HttpClientError`, `RateLimitError`, `ServerError`              |

---

### Layer 2: Connectors (API-Specific)

**Location**: `connectors/{api_name}/`

**Purpose**: Implement API-specific behavior using HTTP tools and define **Read Contracts**.

#### Structure

```
connectors/
└── brevo/                    # Example: Brevo API
    ├── client.py             # API routes implementation
    ├── auth.py               # Brevo-specific auth (API key)
    ├── limiter.py            # Brevo-specific rate limiting
    ├── schemas.py            # Pydantic models for API responses (Read Contract)
    ├── config.py             # API keys, endpoints
    └── README.md             # API-specific documentation
```

#### Responsibilities

| File           | Responsibility       | Example                                                   |
| -------------- | -------------------- | --------------------------------------------------------- |
| `client.py`  | Implement API routes | `get_email_campaigns()`, `get_smtp_templates()`       |
| `schemas.py` | API Data Contracts   | `ApiCampaign`, `ApiEvent` (Validates raw response)    |
| `auth.py`    | API authentication   | `BrevoAuthManager` (API key in header)                  |
| `limiter.py` | API rate limiting    | `BrevoRateLimiter` (uses `x-sib-ratelimit-*` headers) |

#### Design Principles

- **Anti-Corruption Layer**: `schemas.py` validates and normalizes "dirty" API data (e.g., converting dicts to lists, handling inconsistent types) before it enters the workflow.
- **Single Responsibility**: Each connector handles ONE API.

---

### Layer 3: Factories (Assembly)

**Location**: `factories/`

**Purpose**: Assemble connectors with strategies via dependency injection.

#### Example: `factories/brevo.py`

```python
class BrevoFactory:
    @staticmethod
    def create_connector(...) -> BrevoConnector:
        # Assembles Auth, RateLimiter, RetryStrategy, CircuitBreaker
        # Returns fully configured connector
```

---

### Layer 4: Workflows (Business Logic)

**Location**: `workflows/{api_name}/`

**Purpose**: Implement ETL workflows using connectors and define **Write Contracts**.

#### Structure

```
workflows/
└── brevo/
    ├── main.py              # CLI entry point (Typer)
    ├── tasks.py             # ETL tasks (extract logic)
    ├── schemas.py           # Pydantic models for BigQuery (Write Contract)
    ├── transform.py         # Data transformation (Read Model -> Write Model)
    ├── load.py              # BigQuery/GCS loading
    └── config.py            # Job-specific config (tables)
```

#### Responsibilities

| File             | Responsibility     | Example                                                 |
| ---------------- | ------------------ | ------------------------------------------------------- |
| `tasks.py`     | Extract & Validate | Fetch data, validate using `connectors.schemas`       |
| `schemas.py`   | Write Contract     | `CleanCampaign` (Defines target BigQuery schema)      |
| `transform.py` | Transform          | Map `ApiCampaign` -> `CleanCampaign` -> DataFrame   |
| `load.py`      | Load               | Auto-generate BQ schema from `CleanCampaign` and load |

---

## Validation Layer

We enforce a strict **Dual-Schema Architecture** using Pydantic V2 to ensure data integrity and type safety.

### Philosophy: Read vs Write Contracts

#### 1. Read Contract (`connectors/{api_name}/schemas.py`)

* **Purpose:** Mirrors the **Raw API Response** strictly.
* **Goal:** Fail fast if the API changes (e.g., field renaming) and normalize inconsistencies (e.g., list vs dict).
* **Validation:** Permissive on types (Optional fields), strict on structure.
* **Example:**
  ```python
  class ApiResultItem(BaseModel):
      # Validator handles API returning dict OR list
      article: List[ApiArticle]

      @field_validator("article", mode="before")
      def normalize(cls, v): ...
  ```

#### 2. Write Contract (`workflows/{api_name}/schemas.py`)

* **Purpose:** Mirrors the **Target Database Schema** (BigQuery).
* **Goal:** Ensure clean, typed data before loading.
* **Validation:** Strict types, clean field names, business logic constraints.
* **Example:**
  ```python
  class CleanArticle(BaseModel):
      ean: str
      processed_at: datetime # Maps to DATETIME/TIMESTAMP
  ```

### Dynamic Schema Generation

We **DO NOT** hardcode BigQuery schemas in dictionaries. We generate them dynamically from the Write Contract using `utils.schemas.pydantic_to_bigquery_schema`.

**Workflow:**

1. Define `CleanModel` in `workflows/schemas.py`.
2. In `load.py`, generate schema:
   ```python
   bq_schema = pydantic_to_bigquery_schema(CleanModel)
   client.create_table(..., schema=bq_schema)
   ```
3. **Result:** Your Pydantic model is the **Single Source of Truth**.

---

## Implementation Guide: Adding New APIs

Follow these steps to integrate a new API (e.g., SendGrid).

### Step 1: Create Connector Structure & Schemas

```bash
mkdir -p connectors/sendgrid
touch connectors/sendgrid/__init__.py
touch connectors/sendgrid/client.py
touch connectors/sendgrid/schemas.py
touch connectors/sendgrid/auth.py
...
```

**`connectors/sendgrid/schemas.py`**:

```python
from pydantic import BaseModel, Field

class ApiCampaign(BaseModel):
    id: int
    subject: str = Field(..., alias="subject_line")
```

### Step 2: Implement Client

**`connectors/sendgrid/client.py`**:

```python
class SendGridConnector:
    def get_campaigns(self): # route we need to call
        # Return raw response or dict, validation happens in Task
        return self.client.request("GET", "/campaigns")
```

### Step 3: Create Job & Write Schemas

```bash
mkdir -p jobs/sendgrid
touch jobs/sendgrid/schemas.py
...
```

**`jobs/sendgrid/schemas.py`**:

```python
from pydantic import BaseModel
from datetime import datetime

class CleanCampaign(BaseModel):
    campaign_id: int
    subject: str
    ingested_at: datetime
```

### Step 4: Implement Transform

**`jobs/sendgrid/transform.py`**:

```python
from connectors.sendgrid.schemas import ApiCampaign
from jobs.sendgrid.schemas import CleanCampaign

def transform(api_data: List[ApiCampaign]) -> pd.DataFrame:
    clean_data = [
        CleanCampaign(
            campaign_id=c.id,
            subject=c.subject,
            ingested_at=datetime.now()
        ).model_dump()
        for c in api_data
    ]
    # DRY: Use model fields for columns
    return pd.DataFrame(clean_data, columns=CleanCampaign.model_fields.keys())
```

### Step 5: Implement Task

**`jobs/sendgrid/tasks.py`**:

```python
from connectors.sendgrid.schemas import ApiCampaign
from utils.schemas import pydantic_to_bigquery_schema
from jobs.sendgrid.schemas import CleanCampaign

def run_etl(connector):
    # 1. Extract
    resp = connector.get_campaigns()

    # 2. Validate (Read Contract)
    # Fail Fast: Raise exception on validation error
    try:
        valid_campaigns = [ApiCampaign(**item) for item in resp.json()]
    except ValidationError as e:
        logger.error(f"🛑 Validation failed: {e}")
        raise e

    # 3. Transform
    df = transform(valid_campaigns)

    # 4. Load (Schema from Write Contract)
    bq_schema = pydantic_to_bigquery_schema(CleanCampaign)
    save_to_bq(df, schema=bq_schema)
```

---

## Cross-Cutting Concerns

### Error Handling Philosophy

#### Principles

1. **Typed Exceptions**: Use custom exception hierarchy (`http_tools.exceptions`).
2. **Fail Fast**: Propagate errors up.
3. **Graceful Degradation**: Continue processing batches if possible (e.g., skip invalid items).
4. **Validation Errors**: Catch `pydantic.ValidationError` in tasks and **raise exceptions** to fail fast on data contract violations. Logs should clearly indicate the validation error.

---

## Examples

### Example: Validation in Action

```python
try:
    campaign = ApiCampaign(**raw_data)
except ValidationError as e:
    logger.error(f"🛑 Schema validation failed for campaign {raw_data.get('id')}: {e}")
    raise e
```

---

## Testing Strategy

(See [Testing Strategy](#testing-strategy) section above)
