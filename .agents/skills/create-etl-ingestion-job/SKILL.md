---
name: create-etl-ingestion-job
description: "Scaffold or standardize a data-ingestion ETL job (external API -> BigQuery) following the CURRENT team pattern, modeled on external/appfollow. Use when the user wants to create a new ingestion job, add a new external source, or bring an existing job in line with current practices (Typer CLI, Secret Manager, BigQuery load, schema + partitioning)."
argument-hint: "Source name (e.g. 'appfollow') and the BigQuery table(s) to produce"
---

# Create ETL Ingestion Job (current pattern)


Scaffolds a new ingestion job (external API → BigQuery) under
`jobs/etl_jobs/external/<source>/`, or standardizes an existing one, following
the conventions currently used across ETL jobs.

## When to use
- Creating a new external-source ingestion job.
- Standardizing an existing job that drifted from the pattern.

## Prerequisites — gather interactively before writing any code
Do NOT assume or invent these values. Ask the user for them **interactively,
one topic at a time**, and wait for the answer before moving on. Only start
scaffolding (Step 1) once every item below is confirmed. If an answer is
missing or ambiguous, ask a follow-up rather than guessing.

1. **Source name** (lowercase, e.g. `appfollow`). The job lives under
   `jobs/etl_jobs/external/<source>/` (third-party API).
2. **Datasets & endpoints**: for each dataset/table, which API endpoint(s) feed
   it, their path, HTTP method and required params (date range, ids, store…),
   and the shape of the JSON response (where the records live, e.g.
   `response["reviews"]["list"]`). This drives one method per endpoint in
   `client.py` and one `extract_*` per dataset.
3. **Destination table(s)** and, for each, the **BigQuery schema** (column → type).
4. **Field mapping**: for each table, how raw API fields map to the schema
   columns (`raw_field -> column`, plus any cast/rename/default). This drives the
   `transform_*` functions.
5. **Auth**: secret id in Secret Manager and the auth header/scheme. The secret
   id suffix is always `-{ENV_SHORT_NAME}`, but the name and the auth header vary
   per source (e.g. a custom header like `X-...-Api-Token`, or
   `Authorization: Bearer <token>`). Confirm the exact secret id and header.
6. **Idempotency need** (critical — see "Idempotency" below):
   - single entity per partition → partition overwrite (`table$yyyymmdd`,
     `WRITE_TRUNCATE`), or
   - multiple entities share a partition → delete-window-then-append.
7. **Pagination style** of the API (page number, offset, cursor, or single call).

## Target structure

```
jobs/etl_jobs/external/<source>/
├── main.py         # Typer entrypoint, error-handling wrapper
├── client.py       # API client: requests.Session + retry, custom API error
├── etl.py          # ETL class: extract / transform / load + run_etl
├── utils.py        # secrets access, BigQuery load, schema(s), constants
├── pyproject.toml  # pinned deps + ruff + tool.uv constraint-dependencies
└── uv.lock         # generated via `uv lock`
```

Use the templates in [`./references/templates/`](./references/templates/) as the
starting point and fill in the `{{PLACEHOLDERS}}`.

## Procedure

### Step 0 — Collect prerequisites interactively
Walk the user through the 7 prerequisites above, **asking interactively and
waiting for each answer**. Summarize the collected brief back to the user and
get confirmation before generating anything. Do not proceed to Step 1 with any
missing or guessed value.

### Step 1 — Scaffold from templates
Copy the five template files into `external/<source>/`, replacing placeholders:
- `{{SOURCE}}` → PascalCase (e.g. `AppFollow`)
- `{{source}}` → lowercase (e.g. `appfollow`)
- `{{SOURCE_UPPER}}` → UPPER_SNAKE for module-level constants (e.g. `APPFOLLOW`)
- `{{BASE_URL}}`, `{{AUTH_HEADER}}`, secret id, table names, schema dicts.

### Step 2 — Implement the client (`client.py`)
- One class `{{SOURCE}}Client` wrapping `requests.Session`.
- Attach a retry strategy (`urllib3.Retry` + `HTTPAdapter`) covering
  `429, 500, 502, 503, 504`.
- Set auth header from the injected secret (wrap the token in `SecretStr`).
- One method per endpoint; raise a custom `{{SOURCE}}APIError` on non-200.
- Keep pagination loops here (or in `etl.py`) as helper methods.

### Step 3 — Implement extract/transform/load (`etl.py`)
- Class `{{SOURCE}}ETL(client)` with, per dataset:
  `extract_*` → `transform_*` (raw dicts → `pd.DataFrame`) → `load_*`.
- `run_etl(...)` iterates datasets, catches per-dataset exceptions, returns a
  success bool (do NOT let one dataset failure abort the others silently —
  log and continue, then fail the job if any failed).

### Step 4 — Implement loading + schema + secrets (`utils.py`)
- `access_secret_data(project_id, secret_id, ...)` + `get_api_token()` using
  `GCP_PROJECT_ID` / `ENV_SHORT_NAME` env vars; dataset = `raw_{ENV_SHORT_NAME}`.
- Define each table schema as a `dict[col -> BQ type string]`.
- Implement the BigQuery load following the chosen **idempotency** strategy
  (see below). Always set `schema_update_options=[ALLOW_FIELD_ADDITION]` and
  daily `time_partitioning` on the date column.

### Step 5 — CLI (`main.py`)
- Typer command with `start_date` / `end_date` (`YYYY-MM-DD`) options and any
  source-specific params (e.g. `ext_id`, `dataset`).
- Wrap the whole body in the standard error handler:
  ```python
  try:
      ...  # etl workflow
  except typer.Exit:
      raise
  except Exception as e:
      logger.exception(f"ETL job failed: {e}")
      raise typer.Exit(code=1) from e
  ```

### Step 6 — Dependencies
- Fill `pyproject.toml` from the template (pinned `google-cloud-bigquery`,
  `pandas`, `requests`, `typer>=0.24`, etc.) and keep the
  `[tool.uv] constraint-dependencies` block for known CVEs.
- Run `uv lock` in the job folder to generate `uv.lock`.

### Step 7 — Validate
- No syntax/import errors.
- CLI runs: `uv run main.py --help`.
- A dry run against a scratch dataset produces the expected table + schema.
- Re-running the same window is **idempotent** (no duplicates).

## Idempotency (most important design choice)

Pick ONE based on how the data maps to partitions:

| Situation | Strategy | Reference |
|---|---|---|
| One logical entity owns a full daily partition | Write to `table$yyyymmdd` with `WRITE_TRUNCATE` | `external/zendesk/utils.py::save_to_bq` |
| Several entities (e.g. apps) share a partition | DELETE rows for this entity over the window, then `WRITE_APPEND` | `external/appfollow/utils.py::replace_app_rows_in_bq` |

Never blindly `WRITE_TRUNCATE` a shared partition — it erases the other
entities' rows.

## Conventions reference
See [`./references/conventions.md`](./references/conventions.md) for the full
list (naming, env vars, logging, error handling, schema types, CVE constraints).

## Common pitfalls

| Pitfall | Fix |
|---|---|
| Typer swallows exceptions | Wrap body in `try/except`, re-raise `typer.Exit(code=1)` |
| `typer < 0.24` breaks arg parsing in container | Pin `typer>=0.24` |
| `WRITE_TRUNCATE` on a shared partition | Use delete-window-then-append |
| Secrets printed in logs | Wrap tokens in `SecretStr` |
| Missing `ALLOW_FIELD_ADDITION` | Schema changes break the load job |
| Per-dataset failure aborts everything | Catch per dataset, continue, fail job at the end |
```
