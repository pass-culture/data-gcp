# Copilot Instructions for Titelive ETL Project

## Project Overview

This is a two-stage ETL pipeline for extracting and processing data from the
Titelive API (French book/music distribution platform). The architecture
follows a clear separation: extract raw JSON data, then parse/flatten it
into structured format.

## Architecture & Data Flow

- **Stage 1**: `extract_new_offers_from_titelive.py` → Raw Parquet with JSON strings
- **Stage 2**: `parse_offers.py` → Flattened/structured Parquet
- **Token Management**: Global singleton pattern with automatic refresh on 401 errors
- **Data Storage**: All intermediate files in `data/` directory, Parquet format only

## Essential Patterns

### Authentication & API Calls

- Credentials stored in GCP Secret Manager (`titelive_epagine_api_username`, `titelive_epagine_api_password`)
- Global `TOKEN` variable with automatic refresh in `src.utils.requests`
- UTF-8 encoding mandatory for French content (`RESPONSE_ENCODING = "utf-8"`)
- API requires DD/MM/YYYY dates despite YYYY-MM-DD CLI input

### Data Processing Pipeline

```python
# Stage 1: Raw extraction pattern
get_modified_offers(category, date).to_parquet(output_file)

# Stage 2: Parse pattern - explode nested articles
exploded_df = df.assign(article_list=lambda _df: _df.article.map(lambda o: list(o.values()))).explode("article_list")
merged_df = pd.concat([base_columns, article_columns.add_prefix("article_")], axis=1)
```

### CLI & Environment

- **uv** for dependency management (not pip/poetry)
- **Typer** for all CLI interfaces with required options pattern
- **PYTHONPATH=.** required for all test/script execution
- Environment setup: `make install` then `uv run` for execution

## Development Workflows

### Testing Commands

```bash
make test           # All tests
make test-unit      # Unit tests only
make test-integration  # Integration tests only
make test-coverage  # With coverage report
```

### Daily ETL Commands

```bash
make get_books_from_yesterday     # Extract yesterday's books
make parse_products_from_yesterday # Parse yesterday's data
```

### Key Testing Patterns

- **Integration tests**: Mock `get_modified_offers()` function, test full CLI workflow
- **Fixtures**: Use `tests/data/sample_responses.json` for realistic API responses
- **Import isolation**: Import scripts inside test methods to avoid GCP
  credential issues during test discovery

## Critical Configuration

- **Categories**: `LIVRE="paper"`, `MUSIQUE_ENREGISTREE="music"` (constants mapping)
- **Rate limiting**: `RESULTS_PER_PAGE=120`, `MAX_RESPONSES=1e6`
- **Date handling**: CLI accepts YYYY-MM-DD, API requires DD/MM/YYYY conversion
- **JSON serialization**: Complex objects (dicts/lists) → JSON strings for
  downstream compatibility

## Code Standards

- **Ruff**: Formatting and linting (Google docstring convention)
- **Pandas method chaining**: Prefer `.pipe()` and `.assign()` patterns
- **Global state**: Acceptable for `TOKEN` management only
- **Error handling**: Comprehensive with specific exception types and automatic retries

## Integration Points

- **GCP Secret Manager**: All credentials via `src.utils.gcp.access_secret_data()`
- **Titelive API**: Bearer token auth, search and EAN endpoints
- **Data dependencies**: Stage 2 requires Stage 1 output (Parquet with JSON
  data column)

## File Organization

- `scripts/`: CLI entry points with Typer apps
- `src/utils/`: Reusable utilities (gcp, requests, parsing)
- `src/constants.py`: API configuration and enum mappings
- `tests/unit/`: Component-level tests
- `tests/integration/`: End-to-end workflow tests
- `data/`: Transient data storage (not version controlled)
