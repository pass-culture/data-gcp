# allocinev2 — Allocine ELT pipeline

Idempotent ELT pipeline that syncs movie metadata and poster images from the Allocine GraphQL API to BigQuery and GCS.

---

## Overview

The pipeline is split into two independent CLI commands that can be run separately or chained together:

```
sync-movies   →   Allocine API → staging_movies → raw_movies (BigQuery)
sync-posters  →   raw_movies (pending rows) → HTTP download → GCS
```

---

## Commands

### `sync-movies`

Fetches the full movie catalogue from the Allocine API and performs an upsert into BigQuery.

**Steps:**

1. **Extract** — Paginates through the Allocine GraphQL API (`/query/movieList`) using cursor-based pagination, 100 movies per page, with a token-bucket rate limiter (100 req / 60 s).
2. **Transform** — For each movie node:
   - Parses ISO 8601 runtime to integer minutes.
   - Sanitizes HTML entities in text fields.
   - Normalizes nested `cast` and `credits` edges into flat lists of dicts.
   - Serializes all complex columns (lists/dicts) to JSON strings for BigQuery compatibility.
   - Computes a deterministic MD5 `content_hash` over all fields (sorted keys) **before** serialization, so the hash reflects semantic values.
3. **Load** — Truncates `staging_movies` and bulk-loads all transformed rows (WRITE_TRUNCATE).
4. **Merge** — Executes a BigQuery MERGE from `staging_movies` into `raw_movies`:
   - **New movie** (`movie_id` not in raw): INSERT with `poster_to_download = TRUE`, `retry_count = 0`, `poster_gcs_path = NULL`.
   - **Changed movie** (`content_hash` differs): UPDATE all metadata fields. If `poster_url` also changed, additionally reset `poster_to_download = TRUE` and `retry_count = 0`.
   - **Unchanged movie**: no-op.
   - **Deleted movie** (missing from staging): row is **kept** in raw (no deletes).

**Idempotency:** Running `sync-movies` multiple times is safe. Unchanged movies are untouched; the staging table is always fully replaced; `raw_movies` only changes where data actually changed.

---

### `sync-posters`

Downloads poster images for movies flagged for download and stores them in GCS.

**Steps:**

1. **Fetch pending** — Queries `raw_movies` for rows where `poster_to_download = TRUE AND retry_count < max_retries` (default `max_retries = 3`).
2. **For each row:**
   - Generates a deterministic UUID5 from the `poster_url` (same URL always produces the same filename).
   - Downloads the image via HTTP (`httpx`, 60 s timeout, follows redirects).
   - Detects file extension from the URL path, falling back to the `Content-Type` response header, then `jpg`.
   - Uploads to GCS at `gs://{bucket}/posters/{uuid}.{ext}`.
3. **State update (per-row):**
   - **Success** → `poster_to_download = FALSE`, `poster_gcs_path = <gcs_uri>`, `retry_count = 0`.
   - **Failure** → `retry_count += 1`. The row will be retried on the next run until `retry_count` reaches `max_retries`.

**Idempotency:** If a run is interrupted, only the completed rows have been updated. Remaining rows keep `poster_to_download = TRUE` and will be picked up on the next run. A movie whose `poster_url` changes will have its UUID change too, producing a new GCS object; the old object is left in place.

---

## File responsibilities

| File | Responsibility |
|---|---|
| `constants.py` | All environment-driven config: GCP project, dataset, bucket, table names, API URL, rate-limit parameters. |
| `client.py` | `AllocineClient` — HTTP client wrapping `httpx` with token-bucket rate limiting, automatic 429 backoff, and cursor-based pagination. |
| `data.py` | Pure transformation functions: `transform_movie`, `compute_hash`, `normalize_cast`, `normalize_credits`, `parse_runtime_to_minutes`, `sanitize_text`. No I/O. |
| `gcp.py` | All GCP I/O: Secret Manager (API key retrieval), BigQuery (staging load, MERGE, poster state updates), GCS (image upload). Schema definitions live here. |
| `main.py` | CLI entry point (`typer`). Wires the above modules together for `sync-movies` and `sync-posters`. |

---

## Configuration

All configuration is driven by the `ENV_SHORT_NAME` environment variable:

| Variable | `dev` | `prod` |
|---|---|---|
| GCP project | `passculture-data-ehp` | `passculture-data-prod` |
| BigQuery dataset | `raw_dev` | `raw_prod` |
| GCS bucket | `de-lake-dev` | `de-lake-prod` |
| Secret Manager ID | `allocine-data-dev-secret-token` | `allocine-data-prod-secret-token` |

---

## BigQuery schema

### `staging_movies` (transient)

Truncated and reloaded on every `sync-movies` run.

| Column | Type | Notes |
|---|---|---|
| `movie_id` | STRING (PK) | Allocine `id` |
| `internalId` | STRING | |
| `title` | STRING | |
| `originalTitle` | STRING | |
| `type` | STRING | |
| `runtime` | INTEGER | Minutes |
| `synopsis` | STRING | |
| `poster_url` | STRING | |
| `backlink_url` | STRING | |
| `backlink_label` | STRING | |
| `data_eidr` | STRING | |
| `data_productionYear` | INTEGER | |
| `cast_normalized` | STRING | JSON array |
| `credits_normalized` | STRING | JSON array |
| `releases` | STRING | JSON array |
| `countries` | STRING | JSON array |
| `genres` | STRING | JSON array |
| `companies` | STRING | JSON array |
| `content_hash` | STRING | MD5 of all fields |

### `raw_movies` (source of truth)

All columns from `staging_movies` plus:

| Column | Type | Notes |
|---|---|---|
| `poster_to_download` | BOOL | Set `TRUE` on insert or URL change |
| `poster_gcs_path` | STRING | `gs://` URI, populated by `sync-posters` |
| `retry_count` | INTEGER | Incremented on failed download attempts |

---

## Installation & usage

```bash
# Install
uv sync

# Run
uv run python main.py sync-movies
uv run python main.py sync-posters --max-retries 5
```

Or after installing the package:

```bash
allocinev2 sync-movies
allocinev2 sync-posters --max-retries 5
```
