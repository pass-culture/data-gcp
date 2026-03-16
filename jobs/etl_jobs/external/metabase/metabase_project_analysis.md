# Metabase Migration Tool - Complete Project Analysis

## Project Overview

This is an **automated Metabase card (question/dashboard) migration tool** designed for PassCulture. It enables mass updates of Metabase cards when BigQuery tables, schemas, or columns are renamed. The tool handles both native SQL cards and query builder cards without manual intervention.

**Purpose:** Prevent dashboard breakage during BigQuery refactoring by automatically updating all Metabase card references.

---

## 1. JUSTFILE - All Recipes

Located: `justfile`

### Code Quality & Testing
- **`format`** - Format code with ruff
- **`format-check`** - Check formatting without applying changes
- **`lint`** - Fix lint issues (includes import reordering via isort rules)
- **`lint-check`** - Run lint checks without fixing
- **`test *ARGS`** - Run all tests with optional arguments
- **`test-cov`** - Run tests with coverage report (term-missing)
- **`typecheck`** - Run mypy type checking
- **`check-all`** - Run all checks: format-check, lint-check, typecheck, test
- **`fix-all`** - Auto-fix everything: format + lint

### Dependency Management
- **`sync`** - Sync project dependencies (uv sync --group dev)

### Docker Lifecycle
- **`docker-up`** - Start Docker containers in background
- **`docker-down`** - Stop Docker containers (preserves volumes)
- **`docker-reset`** - Stop and destroy volumes (clean slate)
- **`docker-logs`** - Follow Docker container logs
- **`docker-setup`** - Bootstrap local Metabase: start containers + seed data + create cards
  - Runs `scripts/docker-setup.sh`
- **`docker-open`** - Open Metabase in browser (fallback for different OS)

### Integration Testing (Local Docker)
- **`docker-prepare-migration`** - **KEY RECIPE**
  - Executes 2 phases:
    1. Renames table `analytics.old_user_stats` → `enriched_user_data`
    2. Renames column `booking_cnt` → `total_individual_bookings`
    3. Triggers Metabase sync via API
  - Authenticates with hardcoded local credentials (admin@test.com/Test1234!)

- **`docker-test-migrate`** - **KEY RECIPE** (Referenced by user)
  - Executes migration dry-run against local Docker Metabase (no GCP required)
  - Step 1: Get session token from local Metabase
  - Step 2: Fetch all card IDs from Metabase API
  - Step 3: Run migration command with:
    ```
    uv run python main.py migrate \
      --legacy-table-name old_user_stats \
      --new-table-name enriched_user_data \
      --legacy-schema-name analytics \
      --new-schema-name analytics \
      --card-ids "$CARD_IDS" \
      --dry-run
    ```

### API Development
- **`docker-fetch-openapi`** - Download Metabase OpenAPI spec to `api/openapi.json`

---

## 2. MAIN.PY - CLI Structure

Located: `main.py` (10,221 bytes)

### Entry Point & Framework
- **Framework:** Typer (CLI with async support)
- **Logging:** Configured to output `%(asctime)s [%(levelname)s] %(name)s: %(message)s`
- **Single Command:** `migrate` (no subcommands currently)

### Main Command: `migrate`

**Function Signature:**
```python
@app.command()
def migrate(
    legacy_table_name: str,      # Required: legacy table name
    new_table_name: str,          # Required: new table name
    legacy_schema_name: str,      # Required: legacy schema (dataset)
    new_schema_name: str,         # Required: new schema
    dry_run: bool = False,        # Optional: --dry-run flag
    mappings_file: str = "data/mappings.json",  # Optional: path to column mappings
    card_ids_str: str = "",       # Optional: comma-separated card IDs (skips BigQuery)
) -> None
```

### Execution Flow

**Phase 1: Load Column Mappings**
- Tries to load `data/mappings.json`
- Structure: `{legacy_table_name: {old_column_name: new_column_name}}`
- Warns if file doesn't exist but continues

**Phase 2: Connect to Services**
- Calls `get_metabase_host()` from config
- Attempts GCP OAuth2 if `use_bigquery=True`:
  - Uses `google.auth.transport.requests.Request`
  - Fetches ID token via `google.oauth2.id_token.fetch_id_token()`
  - Falls back to direct auth if GCP unavailable
- Authenticates with `MetabaseClient.from_credentials()` using:
  - Username: `METABASE_API_USERNAME` ("metabase-data-bot@passculture.app")
  - Password: from config (env var or GCP Secret Manager)
  - Bearer token: GCP OAuth2 token (optional)

**Phase 3: Discover Impacted Cards**
- Two modes:
  - **BigQuery Discovery** (default if no --card-ids): 
    - Queries `{project}.{INT_METABASE_DATASET}.card_dependency` table
    - Returns list of card IDs sorted by usage
  - **Direct Card IDs** (if --card-ids provided):
    - Parses comma-separated card IDs
    - Skips BigQuery entirely (useful for local testing)

**Phase 4: Build Field Mappings**
- Finds legacy and new table IDs by name/schema
- Calls `build_field_mapping()` which:
  - Gets fields from legacy table
  - Gets fields from new table
  - Applies column_mapping (if table columns were renamed)
  - Creates {old_field_id: new_field_id} dict
- Also creates {old_table_id: new_table_id} dict

**Phase 5: Migrate Cards** (Main Loop)
```
FOR each card_id:
  1. Fetch original card via API
  2. Call migrate_card() to get migrated version
  3. If --dry-run:
     - Print diff via _print_diff()
  4. Else:
     - POST updated card via API
  5. Log result (success/failure) to transition_logs
```

**Phase 6: Log Results**
- If not dry-run AND using BigQuery:
  - Inserts logs to `{project}.{INT_METABASE_DATASET}.migration_log` via BigQuery

### Helper Functions

**`_print_diff(original, migrated, card_id)`**
- Recursively compares original and migrated card structures
- Prints human-readable diff showing before/after for each field that changed
- Format:
  ```
  ==============================================================
  Card 42: Card Name
  Type: native
  ==============================================================
    dataset_query.native.query:
      - "SELECT ... FROM old_user_stats"
      + "SELECT ... FROM enriched_user_data"
  ```

**`_diff_recursive(old, new, path)`**
- Recursively walks dict/list structures
- Only prints differences (skips unchanged values)
- Handles nested dicts and lists

**`_log_to_bigquery(bq_client, logs, project_name, dataset)`**
- Inserts log entries to migration_log table
- Logs contain:
  - card_id, card_name, card_type
  - legacy_table_name, new_table_name
  - timestamp (UTC ISO format)
  - dry_run boolean
  - success boolean
  - error message (if failed)

### Return Codes
- `0`: Success (all cards migrated)
- `1`: At least one card failed to migrate

---

## 3. DOCKER DIRECTORY CONTENTS

Located: `docker/`

### docker-compose.yml

**Services:**

1. **postgres** (Metabase application database)
   - Image: postgres:16-alpine
   - Container Name: postgres
   - Environment:
     - POSTGRES_DB: metabase
     - POSTGRES_USER: metabase
     - POSTGRES_PASSWORD: metabase
   - Port: 5432:5432
   - Volume: metabase-data (persistent)
   - Healthcheck: `pg_isready -U metabase`

2. **sample-db** (Test/sample database - simulates BigQuery)
   - Image: postgres:16-alpine
   - Environment:
     - POSTGRES_DB: sample
     - POSTGRES_USER: sample
     - POSTGRES_PASSWORD: sample
   - Port: 5433:5432 (mapped to 5432 inside)
   - Volume: sample-data (persistent)
   - Healthcheck: `pg_isready -U sample`

3. **metabase** (Metabase application)
   - Image: metabase/metabase:v0.57.15
   - Port: 3000:3000
   - Environment:
     - MB_DB_TYPE: postgres
     - MB_DB_HOST: postgres
     - MB_DB_PORT: 5432
     - MB_DB_DBNAME: metabase
     - MB_DB_USER: metabase
     - MB_DB_PASS: metabase
   - Depends on: postgres (service_healthy)
   - Healthcheck:
     - Command: `curl -f http://localhost:3000/api/health`
     - Interval: 10s
     - Timeout: 5s
     - Retries: 20
     - Start period: 30s (waits for startup)

**Volumes:**
- `metabase-data`: PostgreSQL data for Metabase app database
- `sample-data`: PostgreSQL data for sample database

### scripts/docker-setup.sh

**Purpose:** Idempotent bootstrap script for local Metabase development (10,403 bytes)

**6 Phases:**

**Phase 1: Wait for Metabase**
- Polls `http://localhost:3000/api/session/properties` 
- Max wait: 90 seconds
- Checks every 2 seconds

**Phase 2: Admin Account Setup (Idempotent)**
- Checks `setup-token` from session properties
- If setup-token exists: Create admin account with POST `/api/setup`
  - Email: admin@test.com
  - Password: Test1234!
  - First/Last Name: Admin User
  - Site name: Metabase Dev
  - Tracking disabled
- If already set up: Login with POST `/api/session`
- Stores session token in `$TOKEN` variable

**Phase 3: Connect Sample Database (Idempotent)**
- Checks if "Sample DB" already exists via `/api/database`
- If not: Creates connection with:
  - Engine: postgres
  - Host: sample-db (Docker service name)
  - Port: 5432
  - DB: sample
  - User: sample
  - Password: sample
- Gets database ID for later use

**Phase 4: Create Test Tables (Idempotent)**
- Executes SQL via `docker compose exec sample-db psql`:
  ```sql
  CREATE SCHEMA IF NOT EXISTS analytics;
  CREATE TABLE IF NOT EXISTS analytics.old_user_stats (
      user_id INTEGER PRIMARY KEY,
      booking_cnt INTEGER,
      total_amount DECIMAL(10,2),
      last_booking_date DATE,
      user_type VARCHAR(50)
  );
  INSERT INTO analytics.old_user_stats ... ON CONFLICT (user_id) DO NOTHING;
  ```
- Sample data: 3 rows (user_id 1-3 with different booking counts)

**Phase 5: Sync Metabase (Waits for Table Appearance)**
- Triggers sync via POST `/api/database/{db_id}/sync_schema`
- Polls `/api/table` waiting for `old_user_stats` to appear
- Max wait: 30 seconds
- Confirms table ID before proceeding

**Phase 6: Create Test Cards (Idempotent)**
- Checks existing cards by name
- Creates 2 cards if not already present:

  **Card 1: Test Native Card** (Native SQL)
  - Display: table
  - Query: `SELECT user_id, booking_cnt, total_amount FROM analytics.old_user_stats WHERE user_type = {{user_type}}`
  - Template tag: `{{user_type}}` (text type, for filtering)

  **Card 2: Test Query Builder Card** (UI Builder)
  - Display: table
  - Source table: old_user_stats table ID
  - No filters or aggregations

**Summary Output:**
- Prints Metabase URL, login credentials, database/table/card IDs
- Suggests next steps:
  - `just docker-prepare-migration`
  - `just docker-test-migrate`
  - `just docker-open`

---

## 4. PROJECT STRUCTURE

### Directory Layout

```
metabase/
├── api/
│   ├── __init__.py
│   ├── client.py          # MetabaseClient class (HTTP + retry logic)
│   ├── models.py          # Pydantic models (Card, Table, Field, etc.)
│   └── openapi.json       # Downloaded from Metabase (via docker-fetch-openapi)
│
├── discovery/
│   ├── __init__.py
│   └── bigquery.py        # get_impacted_cards(), build_field_mapping()
│
├── migration/
│   ├── __init__.py
│   ├── card.py            # migrate_card(), native/query builder logic
│   └── field_walker.py    # Recursive MBQL tree walker for safe ID replacement
│
├── scripts/
│   └── docker-setup.sh    # Docker bootstrap script (described above)
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py        # pytest fixtures
│   ├── test_card_migration.py
│   ├── test_field_walker.py
│   └── fixtures/          # Test data
│
├── data/
│   └── mappings.json      # Column rename mappings ({old_col: new_col} per table)
│
├── docs/                  # Documentation (progress, guides, setup)
│
├── docker/
│   └── docker-compose.yml
│
├── main.py               # CLI entry point (Typer)
├── config.py             # Configuration & secrets management
├── pyproject.toml        # Project metadata & dependencies
├── justfile              # Development tasks
├── README.md             # Overview (in French)
└── uv.lock              # Locked dependencies
```

### Python Modules (Not in subdirectories - Legacy?)

These appear to be older modules not yet refactored:
- `metabase_api.py` - Legacy Metabase client (superseded by api/client.py)
- `native.py` - Legacy native SQL handling
- `query.py` - Legacy query builder handling
- `table.py` - Legacy table/field mapping
- `utils.py` - Legacy utilities
- `requirements.txt` / `requirements.in` - Old pip format (now using uv)

---

## 5. CONFIGURATION FILES

### pyproject.toml

**Project Metadata:**
- Name: `metabase-migration`
- Version: `0.0.0`
- Description: Automated Metabase card migration tool for BigQuery table/column renames
- Python: `>=3.14` (using Python 3.14+)

**Core Dependencies:**
```
google-auth-oauthlib>=1.3.0         # GCP OAuth2
google-cloud-bigquery>=3.40.1       # BigQuery client
google-cloud-secret-manager>=2.26.0 # GCP Secret Manager
pydantic>=2.12.5                    # Data validation
requests>=2.32.5                    # HTTP client
tenacity>=9.1.4                     # Retry logic
typer>=0.24.1                       # CLI framework
```

**Dev Dependencies:**
```
mypy>=1.19.1        # Type checking
pytest>=9.0.2       # Testing
pytest-cov>=7.0.0   # Coverage reports
ruff>=0.15.5        # Linting & formatting
```

**Tool Configuration:**

1. **pytest**
   - testpaths: ["tests"]
   - pythonpath: ["."]

2. **ruff** (Linter & Formatter)
   - target-version: py314
   - Lint rules: E4, E7, E9, F, I (errors, future, imports)
   - Max doc length: 120 characters
   - Convention: Google docstrings
   - Bans relative imports
   - Strict type checking mode

3. **mypy** (Type Checker)
   - python_version: 3.14
   - warn_return_any: true
   - warn_unused_configs: true
   - disallow_untyped_defs: true (strict mode)

### config.py

**Environment Variables Read:**
- `PROJECT_NAME`: GCP project ID
- `ENV_SHORT_NAME`: "dev", "stg", or "prod" (defaults to "dev")
- `METABASE_HOST`: Direct Metabase URL (env var override)
- `METABASE_PASSWORD`: Direct password (env var override)
- `METABASE_CLIENT_ID`: OAuth2 client ID (env var override)

**GCP Secret Manager Mapping:**
- `metabase_host_{dev|staging|production}` → Metabase URL
- `metabase-{dev|staging|production}_oauth2_client_id` → OAuth2 client ID
- `metabase-api-secret-{dev|stg|prod}` → API password

**Constants:**
- `INT_METABASE_DATASET`: Dynamically set to `int_metabase_{ENV_SHORT_NAME}`
- `METABASE_API_USERNAME`: "metabase-data-bot@passculture.app"

**Functions:**
- `access_secret(project_id, secret_id, default=None)`: Fetch from Secret Manager or return default
- `get_metabase_host()`: Env var → Secret Manager fallback
- `get_metabase_password()`: Env var → Secret Manager fallback
- `get_metabase_client_id()`: Env var → Secret Manager fallback

---

## 6. KEY MODULES DEEP DIVE

### api/client.py - MetabaseClient

**Purpose:** HTTP client for Metabase API with retry logic

**Class: MetabaseClient**

**Constructor:**
```python
def __init__(self, host: str, session_token: str)
```
- Sets up persistent requests.Session with headers:
  - Content-Type: application/json
  - X-Metabase-Session: {token}

**Class Method: from_credentials()**
- Authenticates with username/password
- Optionally adds Bearer token header (for GCP IAP)
- Returns authenticated MetabaseClient instance

**Methods (with automatic retry on RequestException):**

| Method | HTTP | Purpose |
|--------|------|---------|
| `get_card(card_id)` | GET /api/card/{id} | Fetch single card |
| `put_card(card_id, card)` | PUT /api/card/{id} | Update card |
| `get_table_metadata(table_id)` | GET /api/table/{id}/query_metadata | Fetch table with fields |
| `list_tables()` | GET /api/table | List all tables |
| `find_table_id(name, schema)` | (uses list_tables) | Find table ID by name |
| `get_table_fields(table_id)` | (uses get_table_metadata) | Get field list |

**Retry Strategy:**
- Max attempts: 3
- Backoff: Exponential (2–10s)
- Applies to: requests.exceptions.RequestException

### api/models.py - Pydantic Models

All models use `extra="allow"` for forward compatibility and `populate_by_name=True` for alias support.

**Key Models:**

1. **Card**
   - Fields: id, name, description, display, dataset_query, table_id, visualization_settings, result_metadata, collection_id, archived
   - dataset_query: DatasetQuery (type: "native" or "query")

2. **DatasetQuery**
   - Fields: type, database, native, query
   - native: NativeQuery (if type="native")
   - query: QueryBuilderQuery (if type="query")

3. **NativeQuery**
   - Fields: query (SQL string), template_tags (dict)
   - template_tags: {tag_name: TemplateTags}

4. **TemplateTags**
   - Fields: id, name, display_name, type, dimension
   - type: "text", "dimension", etc.
   - dimension: field reference like ["field", 42, null]

5. **QueryBuilderQuery** (MBQL)
   - Fields: source_table, fields, filter, breakout, aggregation, joins, order_by, expressions, limit
   - Represents UI builder query (structured/visual query)

6. **Table**
   - Fields: id, name, schema_, db_id, fields
   - fields: list[MetabaseField]

7. **MetabaseField**
   - Fields: id, name, display_name, base_type, semantic_type, table_id

8. **ResultMetadataColumn**
   - Fields: name, display_name, base_type, field_ref, id

### discovery/bigquery.py

**Functions:**

**`get_impacted_cards(bq_client, legacy_table, legacy_schema, project_name, dataset) → list[int]`**
- Queries: `{project}.{dataset}.card_dependency` table
- Joins with: `{project}.{dataset}.activity` for usage metrics
- Filters: Non-archived cards
- Orders by: total_users (highest first)
- Returns: List of card IDs sorted by impact

**`build_field_mapping(metabase_client, legacy_table_id, new_table_id, column_mapping=None) → dict[int, int]`**
- Fetches fields from both tables
- For each legacy field:
  - Applies column_mapping (if column was renamed)
  - Looks up new field by name
  - Creates mapping: {old_field_id: new_field_id}
- Logs unmapped fields as warnings
- Returns: Field ID mapping dict

**`build_table_mapping(legacy_table_id, new_table_id) → dict[int, int]`**
- Simple helper: `{legacy_table_id: new_table_id}`

### migration/card.py

**Main Function: `migrate_card(card, field_mapping, table_mapping, column_mapping, old_schema, new_schema, old_table, new_table) → Card`**

- Dispatches based on query type:
  - "native" → `migrate_native_query()`
  - "query" → `migrate_query_builder()`
  - Other → logs warning, returns unchanged

- Updates:
  1. dataset_query (via type-specific function)
  2. table_id (top-level)
  3. result_metadata (field refs)
  4. visualization_settings (field refs)

- Returns: New Card object (immutable)

**`migrate_native_query(dataset_query, column_mapping, field_mapping, old_schema, new_schema, old_table, new_table) → dict`**

- Updates SQL string via `_replace_sql_references()`:
  1. `old_schema.old_table` → `new_schema.new_table` (most specific)
  2. `old_table` → `new_table` (word boundary regex)
  3. Column names (regex, skipping lines with `[[...]]`)

- Updates template tags via `_migrate_template_tags()`:
  - For each tag with type="dimension":
    - Updates field ID in dimension array: `["field", old_id, ...]` → `["field", new_id, ...]`

- Returns: Updated dataset_query dict

**`migrate_query_builder(dataset_query, field_mapping, table_mapping) → dict`**
- Uses `replace_field_ids()` (field_walker) on entire structure
- Handles: source-table, field refs, FK refs, filter/aggregation/join references

**Helper Functions:**

- `_replace_sql_references()`: Regex-based SQL string updates
- `_migrate_template_tags()`: Updates dimension field IDs in template tags
- `_migrate_result_metadata()`: Uses field_walker on result_metadata
- `_migrate_visualization_settings()`: Uses field_walker on viz_settings

### migration/field_walker.py

**Purpose:** Recursive MBQL tree walker for safe field ID replacement (avoids dangerous regex-on-JSON)

**Main Function: `replace_field_ids(node, field_mapping, table_mapping=None) → Any`**

- Recursively walks dict/list structures
- Replaces field IDs only in semantic MBQL contexts
- Never mutates input

**MBQL Constructs Handled:**

1. **Field References:** `["field", <id>, <options>]`
   - Replaces `<id>` with mapped value
   - Recurses into options

2. **Foreign Key References:** `["fk->", <field_ref>, <field_ref>]`
   - Recurses into both field refs

3. **Source Table:** `{"source-table": <id>}`
   - Replaces table ID in this context

4. **Generic Recursion:** Lists and dicts
   - Walks all children

**Safety:**
- Only replaces in known MBQL contexts
- Never touches card_id, dashboard_id, parameter_id, etc.
- Unlike legacy regex approach, no false positives

---

## 7. DATA FILES

### data/mappings.json

Structure: `{table_name: {old_column: new_column}}`

Example mapping for table "old_user_stats":
```json
{
  "old_user_stats": {
    "booking_cnt": "total_individual_bookings",
    "...": "..."
  },
  "old_venue_data": {...},
  ...
}
```

**Purpose:** Defines column renames when table is renamed

---

## 8. TESTING

### Test Files

**`tests/test_field_walker.py`**
- Tests `replace_field_ids()` function
- Tests MBQL field ref detection and replacement
- Tests FK reference handling

**`tests/test_card_migration.py`**
- Tests `migrate_card()` function
- Tests native SQL migration
- Tests query builder migration
- Tests diff generation

**`tests/conftest.py`**
- pytest fixtures for test data
- Mock Metabase responses

### Running Tests

```bash
uv run pytest                           # Run all
uv run pytest tests/test_field_walker.py  # Specific file
uv run pytest --cov=. --cov-report=term-missing  # With coverage
```

---

## 9. WORKFLOW EXAMPLES

### Local Testing Workflow

```bash
# 1. Start Docker environment (~60s for health checks)
just docker-up

# 2. Bootstrap with seed data and test cards
just docker-setup

# 3. Prepare migration scenario (rename table/column)
just docker-prepare-migration

# 4. Run migration in dry-run mode (no changes)
just docker-test-migrate

# 5. View output in browser
just docker-open

# 6. Stop and clean
just docker-down
just docker-reset  # Or this to destroy volumes
```

### Production Workflow

```bash
# 1. Set environment
export PROJECT_NAME="passculture-prod"
export ENV_SHORT_NAME="prod"

# 2. Run migration against production Metabase
python main.py migrate \
  --legacy-table-name old_user_stats \
  --new-table-name enriched_user_data \
  --legacy-schema-name analytics \
  --new-schema-name analytics \
  --dry-run

# 3. If dry-run looks good, run for real
python main.py migrate \
  --legacy-table-name old_user_stats \
  --new-table-name enriched_user_data \
  --legacy-schema-name analytics \
  --new-schema-name analytics

# 4. Check logs in BigQuery
# SELECT * FROM int_metabase_prod.migration_log ORDER BY timestamp DESC
```

---

## 10. KEY INSIGHTS

### Strengths

1. **Type-Safe:** Full type hints, Pydantic models, mypy strict mode
2. **Production-Ready:** Retry logic, comprehensive logging, BigQuery audit trail
3. **Dual Authentication:** Supports GCP OAuth2 (production) and direct auth (local testing)
4. **Safe Field Replacement:** Recursive tree walker avoids regex-on-JSON hazards
5. **Both Query Types:** Handles native SQL and query builder cards
6. **Column Rename Support:** Configurable mapping for renamed columns
7. **Immutable Operations:** Returns new objects, never mutates inputs
8. **Dry-Run Mode:** Preview all changes before committing
9. **Local Testing:** Full Docker environment, no GCP credentials needed
10. **Comprehensive Tools:** justfile recipes for all development tasks

### Migration Strategy

The tool uses a **three-level mapping approach:**

1. **Table Mapping:** `old_table_id → new_table_id`
   - Simple, for top-level table references
   
2. **Field Mapping:** `old_field_id → new_field_id`
   - Primary strategy for query builder cards
   - Matches fields by name (with optional column renames)
   
3. **Column Mapping:** `old_column_name → new_column_name`
   - Only for native SQL (regex in SQL strings)
   - Loaded from JSON file, optional

### What Gets Updated

1. **Native SQL Cards:**
   - SQL query string (schema, table, column names)
   - Template tags (dimension field IDs)
   - Result metadata (field refs)

2. **Query Builder Cards:**
   - source-table references
   - Field refs in filters, aggregations, expressions, joins
   - Result metadata (field refs)

3. **Both Card Types:**
   - table_id (top-level)
   - visualization_settings (field refs in column formats, click behaviors)
   - result_metadata (field refs)

### Limitations / Notes

- Legacy modules (`native.py`, `query.py`, `table.py`, `utils.py`) not removed; new code in `api/` and `migration/`
- No support for derived tables or saved questions referencing other cards
- Column mapping is optional (good for schema-only renames)
- BigQuery discovery requires `card_dependency` table (populated externally)

