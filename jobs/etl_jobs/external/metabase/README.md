# Metabase Migration Tool

Automatically updates Metabase cards (questions/dashboards) when BigQuery tables are renamed or columns change names.

When data-gcp renames a BigQuery table or its columns, every Metabase card that references it breaks. This CLI tool discovers impacted cards, builds field-level mappings between the old and new tables via the Metabase API, and rewrites each card in place.

## Supported card types

| Card type | How it's updated |
|-----------|-----------------|
| **Native SQL** (`mbql.stage/native`) | Regex replacement of schema, table, and column names in the SQL text + field ID replacement in template tags |
| **Query builder** (`mbql.stage/mbql`) | Recursive tree walker replaces field IDs and `source-table` IDs throughout the MBQL structure |

Both types also get `table_id`, `result_metadata`, and `visualization_settings` updated.

## Migration input format

Migrations are driven by `data/tables-to-migrate.json`. Each key is `schema.table` and the value describes what changed:

```jsonc
{
  // Table rename + column renames
  "old_schema.old_table": {
    "target_table": "new_schema.new_table",
    "columns_to_migrate": {
      "old_column": "new_column"
    }
  },
  // Column renames only (same table)
  "analytics.some_table": {
    "columns_to_migrate": {
      "old_col": "new_col"
    }
  },
  // Table rename only (no column changes)
  "old_schema.old_table": {
    "target_table": "new_schema.new_table"
  }
}
```

At least one of `target_table` or `columns_to_migrate` must be present per entry.

## Execution flow

```
1. Load & validate tables-to-migrate.json (Pydantic)
2. Authenticate to Metabase (session token, optionally through IAP)
3. Build table catalog (all tables including inactive/deleted)
4. For each table migration entry:
   a. Discover impacted cards (API scan or --card-ids override)
   b. Build field mapping: old_field_id -> new_field_id (by matching column names)
   c. Build table mapping: old_table_id -> new_table_id
   d. Migrate each card (dispatch by stage type)
   e. Log results to BigQuery (production only)
5. Exit 0 on success, 1 if any table failed
```

## CLI usage

```bash
# Dry run (preview diffs, no writes)
uv run python main.py migrate \
  --tables-file data/tables-to-migrate.json \
  --database-name "Data Analytics Production" \
  --dry-run

# Execute migration
uv run python main.py migrate \
  --tables-file data/tables-to-migrate.json \
  --database-name "Data Analytics Production"

# Migrate specific cards only (skips API discovery)
uv run python main.py migrate \
  --tables-file data/tables-to-migrate.json \
  --database-name "Data Analytics Production" \
  --card-ids "42,101,203" \
  --dry-run
```

## Authentication

| Environment | How it works |
|-------------|-------------|
| **Local Docker** | Direct username/password via `METABASE_HOST`, `METABASE_API_USERNAME`, `METABASE_PASSWORD` env vars |
| **Staging (port-forward)** | `kubectl -n metabase-analytics port-forward svc/metabase-analytics 8082:80`, then set `METABASE_HOST=http://localhost:8082`. IAP is bypassed. |
| **Staging/Prod (IAP)** | GCP service account key or Application Default Credentials mint an ID token for IAP. Secrets are fetched from GCP Secret Manager. |

Copy `.env.example` to `.env` for staging/port-forward configuration.

## Local development

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- [just](https://github.com/casey/just)
- Docker & Docker Compose

### Setup

```bash
uv sync --group dev    # or: just sync
```

### Quality checks

```bash
just check-all         # format-check + lint-check + typecheck + tests
just fix-all           # auto-format + auto-fix lint
just test              # pytest
just test-cov          # pytest with coverage
just typecheck         # mypy
```

### Docker integration test (no GCP credentials required)

The Docker environment spins up 3 containers: a Metabase instance (v0.57), its PostgreSQL app database, and a sample PostgreSQL database that simulates BigQuery tables.

```bash
just docker-up                 # Start containers
just docker-setup              # Bootstrap: admin account, sample DB, test tables, test cards
just docker-prepare-migration  # Rename table + column, trigger Metabase sync
just docker-test-migrate       # Run migration --dry-run against local Metabase
just docker-open               # Open Metabase UI in browser
just docker-reset              # Stop containers and destroy volumes (clean slate)
```

### Staging recipes

```bash
just stg-pf-explore                                     # List tables and referencing cards
just stg-pf-full-dry-run "Data Analytics Staging"       # Dry-run all tables
just stg-pf-for-card-id-only-dry-run "42,101" "Data Analytics Staging"  # Dry-run specific cards
just stg-pf-for-card-id-only-execute "42,101" "Data Analytics Staging"  # Execute for specific cards
```

Run `just` with no arguments to see all available recipes.

## Project structure

```
metabase/
├── main.py                 # CLI entry point (Typer). Orchestrates the full flow.
├── config.py               # Environment variables, GCP Secret Manager, IAP token minting.
├── justfile                # Development recipes (quality checks, Docker, staging).
├── pyproject.toml          # Project metadata, dependencies, tool config.
│
├── api/
│   ├── client.py           # MetabaseClient — HTTP client with retry (tenacity).
│   └── models.py           # Pydantic v2 models: Card, DatasetQuery, Table, Field, TablesToMigrate.
│
├── discovery/
│   ├── mapping.py          # build_field_mapping (match fields by name), build_table_mapping.
│   └── metabase.py         # Card dependency discovery via Metabase API. Caches to JSON.
│
├── migration/
│   ├── card.py             # migrate_card dispatcher: native SQL (regex) vs query builder (tree walker).
│   └── field_walker.py     # Recursive MBQL tree walker. Replaces IDs only in semantic contexts.
│
├── data/
│   ├── tables-to-migrate.json      # Migration configuration (schema.table -> target + column renames).
│   └── cache_*.json                # Runtime caches (git-ignored).
│
├── docker/
│   ├── docker-compose.yml          # 3 containers: metabase, postgres (app DB), sample-db.
│   └── tables-to-migrate-test.json # Test migration config for Docker environment.
│
├── scripts/
│   ├── docker-setup.sh             # Idempotent 6-phase bootstrap (wait, admin, DB, tables, sync, cards).
│   ├── docker-test-inactive-tables.sh  # Verify inactive tables appear via metadata API.
│   └── stg_explore.py              # Staging exploration (list tables, find referencing cards).
│
└── tests/                  # pytest suite: card migration, field walker, discovery, client.
```

## Key design decisions

**Tree walker instead of regex-on-JSON.** The legacy approach serialized the entire card to JSON then ran `re.sub(r'\b42\b', '99')`, which could silently corrupt any integer matching a field ID (card IDs, dashboard IDs, parameter IDs...). The tree walker in `field_walker.py` only touches values in recognized MBQL constructs: `["field", {opts}, <id>]`, `["fk->", ...]`, and `{"source-table": <id>}`.

**pMBQL format (Metabase v0.57+).** Since v0.57, the API returns pMBQL (MBQL v5): `dataset_query.stages[]` with `"lib/type": "mbql.stage/native"` or `"mbql.stage/mbql"`, instead of the legacy `type` + `native`/`query` structure.

**Immutability.** All migration functions return new objects. Inputs are never mutated.

**Column renames skip Metabase filter syntax.** In native SQL, lines containing `[[ ... ]]` (Metabase optional filter blocks) are excluded from column renaming to avoid breaking parameterized queries.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PROJECT_NAME` | Prod/Stg | GCP project ID (e.g. `passculture-data-ehp`) |
| `ENV_SHORT_NAME` | Prod/Stg | `dev`, `stg`, or `prod` |
| `METABASE_HOST` | Local/PF | Direct Metabase URL (bypasses IAP + Secret Manager) |
| `METABASE_API_USERNAME` | No | Defaults to `metabase-data-bot@passculture.app` |
| `METABASE_PASSWORD` | Local | Override password when Secret Manager is unavailable |
| `DATABASE_NAME` | Stg scripts | Metabase database display name |
| `GOOGLE_SA_KEY_PATH` | No | Service account key for IAP token minting |
