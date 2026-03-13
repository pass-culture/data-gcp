# CLAUDE.md — Metabase Migration Tool

## Project overview

Automated tool to migrate Metabase cards (saved questions) after BigQuery table/column renames.
Handles both **native SQL cards** (rewrites SQL text) and **query builder cards** (replaces MBQL field/table IDs via a safe tree walker).

## Quick commands

```bash
just sync              # Install dependencies (uv sync --group dev)
just test              # Run all tests
just test-cov          # Tests with coverage
just check-all         # Format + lint + typecheck + tests
just fix-all           # Auto-fix format + lint

just docker-setup      # Full local env: Docker containers + seed data + test cards
just docker-test-migrate  # Dry-run migration against local Docker Metabase

just stg-explore       # Impact report on staging Metabase (requires GCP auth)
just stg-dry-run <LEGACY_TABLE> <NEW_TABLE> <LEGACY_SCHEMA> <NEW_SCHEMA> <CARD_IDS>
```

## Architecture

```
main.py              CLI entry point (Typer). Orchestrates: discover -> map -> migrate -> log
config.py            Settings from env vars + GCP Secret Manager
api/
  client.py          MetabaseClient — HTTP client with retry (tenacity), session auth
  models.py          Pydantic v2 models: Card, DatasetQuery, NativeQuery, QueryBuilderQuery, etc.
discovery/
  bigquery.py        Card discovery via BQ + field/table mapping builders
migration/
  card.py            Card migration: dispatches native vs query builder, updates SQL/MBQL
  field_walker.py    Recursive MBQL tree walker — replaces field/table IDs only in semantic contexts
scripts/
  stg_explore.py     Read-only staging impact report
  docker-setup.sh    Idempotent local Metabase bootstrap (6 phases)
data/
  mappings.json      Column rename mappings: {table_name: {old_col: new_col}}
tests/
  conftest.py        Shared fixtures (field_mapping, table_mapping, column_mapping)
  fixtures/          JSON fixtures for native + query builder cards (MBQL v5)
  test_field_walker.py   42+ tests for the tree walker (critical: non-field integers preserved)
  test_card_migration.py Tests for native SQL + query builder card migration
docker/
  docker-compose.yml   Postgres (Metabase app DB) + Postgres (sample DB) + Metabase v0.57.15
```

## Key design decisions

- **Tree walker over regex**: `field_walker.py` replaces IDs only in `["field", id, ...]` and `{"source-table": id}` contexts. The old regex approach (`re.sub(r'\b42\b', '99')` on JSON) corrupted card_id, dashboard_id, etc.
- **Immutability**: All migration functions return new objects, never mutate inputs.
- **No `__init__.py`**: Uses `explicit_package_bases = true` in mypy config.
- **`extra="allow"`** on all Pydantic models for forward compatibility with new Metabase API fields.

## Python version

Python **3.14** (see `pyproject.toml`). All code uses modern type hints (`dict[str, str]`, `list[int]`, `X | None`).

## Environment variables

| Variable | Purpose |
|---|---|
| `PROJECT_NAME` | GCP project ID (e.g., `passculture-data-ehp` for staging) |
| `ENV_SHORT_NAME` | `dev`, `stg`, or `prod` — drives secret names and dataset |
| `METABASE_HOST` | Override Metabase URL (otherwise from Secret Manager) |
| `METABASE_PASSWORD` | Override password (otherwise from Secret Manager) |
| `METABASE_CLIENT_ID` | Override OAuth2 client ID for IAP (otherwise from Secret Manager) |
| `METABASE_API_USERNAME` | Defaults to `metabase-data-bot@passculture.app` |

## Testing

```bash
just test                    # Unit tests only (no external dependencies)
just docker-setup            # Set up local Metabase in Docker
just docker-prepare-migration # Create renamed table for integration testing
just docker-test-migrate     # Integration test: dry-run against local Docker
```

Test files are in `tests/`. Fixtures are JSON files in `tests/fixtures/`.

## Linting and formatting

- **ruff** for formatting and linting (rules: E4, E7, E9, F, I)
- **mypy** for type checking (strict: `disallow_untyped_defs = true`)
- Target: Python 3.14

## Staging workflow

1. `gcloud auth application-default login`
2. `just stg-explore` — identify impacted cards
3. `just stg-dry-run <args>` — preview changes (read-only)
4. Review diff output
5. `uv run python main.py migrate <args>` — apply (no just recipe, intentional)
6. Verify in staging Metabase UI
