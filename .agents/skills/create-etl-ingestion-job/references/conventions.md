# Conventions — ETL ingestion jobs (current pattern)

> These conventions are extracted from `jobs/etl_jobs/external/appfollow` and the
> surrounding jobs. They describe the current ingestion pattern used across ETL
> jobs.

## Location & structure
- Third-party APIs → `jobs/etl_jobs/external/<source>/`.
- Files: `main.py`, `client.py`, `etl.py`, `utils.py`, `pyproject.toml`, `uv.lock`.

## Naming
- `<source>` is lowercase (folder, package name, secret id, table prefix).
- Classes use PascalCase: `{{SOURCE}}Client`, `{{SOURCE}}ETL`, `{{SOURCE}}APIError`.
- Tables: `<source>_<dataset>` (e.g. `appfollow_reviews`).

## Environment variables (provided by the runtime)
- `GCP_PROJECT_ID` — GCP project.
- `ENV_SHORT_NAME` — `dev` / `stg` / `prod`.
- Destination dataset: `raw_{ENV_SHORT_NAME}`.

## Secrets
- Stored in Secret Manager. The id ends with `-{ENV_SHORT_NAME}`, but the exact
  name varies per source — confirm it (e.g. `appfollow-api-token-{ENV_SHORT_NAME}`
  with header `X-AppFollow-API-Token`, or `<source>-api-key-{ENV_SHORT_NAME}`).
- Access via `access_secret_data(...)`; wrap tokens in `SecretStr` so they are
  never printed in logs.
- `access_secret_data` catches `DefaultCredentialsError` and returns a default
  (so imports don't crash in local/test without credentials).

## HTTP client
- One `requests.Session` with a `urllib3.Retry` + `HTTPAdapter` retry strategy
  covering `429, 500, 502, 503, 504`, `total=3`, `backoff_factor=1`.
- Auth via a session header.
- Raise a custom `{{SOURCE}}APIError` on non-200 responses.
- Keep API-specific pagination in the client or the ETL class.

## CLI (Typer)
- Date options are strings in `YYYY-MM-DD` format (`start_date`, `end_date`).
- Use an `Enum` for a `dataset` selector when the job imports several datasets.
- Always wrap the command body:
  ```python
  try:
      ...
  except typer.Exit:
      raise
  except Exception as e:
      logger.exception(f"ETL job failed: {e}")
      raise typer.Exit(code=1) from e
  ```
- Pin `typer>=0.24` (older versions break arg parsing in the container entrypoint).

## Logging
- `loguru` is the de-facto choice for new jobs (`from loguru import logger`).
  (Some older jobs use `logging`; prefer `loguru` for consistency with appfollow.)

## Schemas & types
- Define each table schema as `dict[column -> BQ type string]`
  (`STRING`, `INTEGER`, `FLOAT`, `DATE`, `DATETIME`, `TIMESTAMP`).
- Convert to `bigquery.SchemaField` via a `to_sql_type` helper.
- Always set `schema_update_options=[ALLOW_FIELD_ADDITION]`.
- Always set daily `time_partitioning` on the date column.

## Idempotency (choose one)
- **Strategy A — partition overwrite**: write to `table$yyyymmdd` with
  `WRITE_TRUNCATE`. Use only when a single logical entity owns the whole
  partition. (`save_to_bq` in the utils template.)
- **Strategy B — delete-window + append**: `DELETE` the entity's rows over
  `[start_date, end_date]`, then `WRITE_APPEND`. Use when several entities share
  a partition (e.g. several apps per day). (`replace_entity_rows_in_bq` in the
  utils template; mirrors `appfollow.replace_app_rows_in_bq`.)

## Error handling in `run_etl`
- Import several datasets in a loop; catch exceptions **per dataset**, log, and
  continue; return `False` if any dataset failed so the job exits non-zero.

## Dependencies
- Use the pinned versions in the `pyproject.toml` template.
- Keep the `[tool.uv] constraint-dependencies` block for known CVEs (in sync with
  the `vulnerability-fix` skill).
- Generate the lock file with `uv lock` in the job folder.
