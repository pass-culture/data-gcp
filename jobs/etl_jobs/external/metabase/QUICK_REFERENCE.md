# Metabase Migration Tool - Quick Reference Guide

## 🎯 What This Tool Does

Automatically updates Metabase cards when BigQuery tables are renamed or columns change names. Handles both:
- **Native SQL cards** (direct SQL queries)
- **Query builder cards** (UI-constructed questions)

## 📋 Command Line Usage

```bash
# Dry-run against local Metabase (no GCP needed)
uv run python main.py migrate \
  --legacy-table-name old_user_stats \
  --new-table-name enriched_user_data \
  --legacy-schema-name analytics \
  --new-schema-name analytics \
  --card-ids "1,2,3" \
  --dry-run

# Production migration (discovers cards via BigQuery)
export PROJECT_NAME="passculture-prod"
export ENV_SHORT_NAME="prod"
uv run python main.py migrate \
  --legacy-table-name old_user_stats \
  --new-table-name enriched_user_data \
  --legacy-schema-name analytics \
  --new-schema-name analytics
```

## 🐳 Local Testing with Docker

```bash
just docker-up              # Start 3 containers: postgres, sample-db, metabase
just docker-setup           # Bootstrap: admin account, DB connection, tables, test cards
just docker-prepare-migration  # Rename table/column in sample-db
just docker-test-migrate    # Run migration dry-run
just docker-open            # Open browser to http://localhost:3000
just docker-reset           # Clean up (destroy volumes)
```

## 📁 Key Files

| File | Purpose |
|------|---------|
| `main.py` | CLI entry point (Typer) |
| `api/client.py` | MetabaseClient (HTTP + retry) |
| `api/models.py` | Pydantic models (Card, Table, Field) |
| `discovery/bigquery.py` | BigQuery discovery + field mapping |
| `migration/card.py` | Main migration logic |
| `migration/field_walker.py` | Safe MBQL tree walker |
| `config.py` | Config management (env vars + GCP Secrets) |
| `data/mappings.json` | Column rename mappings |
| `docker-compose.yml` | 3 services: postgres, sample-db, metabase |
| `scripts/docker-setup.sh` | Bootstrap script (6 phases) |
| `justfile` | Development tasks |

## 🔄 Execution Flow (6 Phases)

```
1. Load Column Mappings        → From data/mappings.json
2. Connect to Services         → Metabase API + GCP OAuth2 (optional)
3. Discover Impacted Cards     → BigQuery OR --card-ids param
4. Build Field Mappings        → old_field_id → new_field_id
5. Migrate Each Card           → dispatch by query type, print diffs or write
6. Log Results                 → BigQuery migration_log table
```

## 🔐 Authentication Modes

### Production (GCP)
```
1. Read env vars: PROJECT_NAME, ENV_SHORT_NAME
2. Try GCP OAuth2: google.oauth2.id_token.fetch_id_token()
3. Fallback to: GCP Secret Manager
   - metabase_host_{env}
   - metabase-api-secret-{env}
   - oauth2_client_id
4. Connect via MetabaseClient.from_credentials()
```

### Local Testing
```
Environment Variables:
- METABASE_HOST="http://localhost:3000"
- METABASE_PASSWORD="Test1234!"

Credentials:
- Username: metabase-data-bot@passculture.app
- Password: Test1234! (hardcoded in docker-setup.sh)
```

## 📊 What Gets Updated

### Native SQL Cards
- SQL query string: `schema.table`, `table`, column names (avoids `[[...]]`)
- Template tags: dimension field IDs
- Result metadata: field references
- Visualization settings: field references
- Top-level table_id

### Query Builder Cards
- source-table reference
- All field references (["field", id, ...])
- FK references (["fk->", ...])
- Filter/aggregation/join references
- Result metadata: field references
- Visualization settings: field references
- Top-level table_id

## 🛡️ Safety Features

1. **Tree Walker** (`field_walker.py`):
   - Only replaces field IDs in MBQL contexts
   - Avoids regex-on-JSON hazards
   - Never mutates inputs

2. **Immutable Operations**:
   - All functions return new objects
   - Original cards preserved

3. **Dry-Run Mode**:
   - Preview all changes before committing
   - No API writes

4. **Retry Logic**:
   - 3 attempts with exponential backoff (2–10s)
   - Handles transient failures

5. **Comprehensive Logging**:
   - Console output: progress + diffs
   - BigQuery: full audit trail

## ⚙️ Configuration

### Environment Variables
```
PROJECT_NAME           # GCP project ID
ENV_SHORT_NAME         # dev / stg / prod (determines SECRET prefix)
METABASE_HOST          # Override: env var before Secrets
METABASE_PASSWORD      # Override: env var before Secrets
METABASE_CLIENT_ID     # OAuth2 client ID (env var before Secrets)
```

### data/mappings.json
```json
{
  "old_user_stats": {
    "booking_cnt": "total_individual_bookings",
    "nb_consult": "total_consulted_help"
  },
  "old_venue_data": {
    "...": "..."
  }
}
```

## 📈 Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=. --cov-report=term-missing

# Run specific test
uv run pytest tests/test_field_walker.py

# Type check
uv run mypy .

# Lint + format
uv run ruff check --fix .
uv run ruff format .
```

## 🚨 Important Notes

1. **Column Mapping is Optional**: Only needed if columns are renamed
2. **BigQuery Discovery**: Requires `card_dependency` table populated externally
3. **Template Tag Dimension**: Only field refs of type "dimension" are updated
4. **Archived Cards**: Filtered out automatically (WHERE card_name NOT LIKE '%archive%')
5. **Usage Sorting**: Cards migrated in order of highest usage first

## 📍 Docker Container Details

| Container | Port | Purpose |
|-----------|------|---------|
| postgres | 5432 | Metabase app database |
| sample-db | 5433 → 5432 | Test data (simulates BigQuery) |
| metabase | 3000 | Metabase UI + API |

Admin credentials (docker-setup creates):
- **Email**: admin@test.com
- **Password**: Test1234!
- **Sample DB**: postgres://sample-db:5432/sample

## 🔍 Troubleshooting

### No cards found
- Check BigQuery `card_dependency` table exists and has data
- Use `--card-ids "1,2,3"` to test with specific IDs

### Migration failed for a card
- Check migration logs in BigQuery `migration_log` table
- Try `--dry-run` first to see diffs
- Inspect the original card in Metabase UI

### Field not mapped
- Check `data/mappings.json` for column names
- Check field names in legacy vs. new table match (after rename)
- Log will show "unmapped fields" warnings

### Metabase timeout
- Increase sleep in docker-setup.sh Phase 1 (default 90s)
- Check `just docker-logs`

## 📝 Justfile Recipes Quick List

```bash
just format          # Format code with ruff
just lint            # Fix lint issues
just typecheck       # mypy type checking
just test            # Run pytest
just check-all       # Run format + lint + type + test

just docker-up       # Start containers
just docker-setup    # Bootstrap Metabase
just docker-prepare-migration  # Rename table/column
just docker-test-migrate       # Run migration --dry-run
just docker-open     # Open browser
just docker-reset    # Clean up
```

## 🎓 Key Concepts

### MBQL (Metabase Query Language)
Metabase's internal representation for query builder questions:
- `["field", 42, null]` → field reference
- `["fk->", [...], [...]]` → foreign key reference
- `{"source-table": 42}` → root table reference

### Field Mapping Strategy
1. Get all fields from legacy table
2. Get all fields from new table
3. For each legacy field:
   - Apply column_mapping if column was renamed
   - Find matching field in new table by name
   - Store old_field_id → new_field_id

### Discovery Flow
1. Query `card_dependency` table → card IDs using legacy table
2. Join with `activity` table → usage metrics
3. Filter archived cards
4. Sort by usage (highest first)

