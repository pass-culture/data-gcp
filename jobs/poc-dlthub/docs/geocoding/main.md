# main.py — Pipeline Documentation

## Purpose

`main.py` implements a batch geocoding pipeline that:

1. Queries user addresses from BigQuery (production data)
2. Sends them in batches to the [Geopf](https://data.geopf.fr/geocodage) geocoding API
3. Loads the geocoded results into a local DuckDB database via [dlt](https://dlthub.com/)

---

## Functions

### `call_geocoding_search_batch_api(rows_batch, is_first_batch)`

**File**: `main.py:14`

Converts a list of BigQuery rows to CSV, writes them to `extracted_addresses.csv`, then POSTs the CSV to the Geopf batch geocoding endpoint.

**Parameters**:
- `rows_batch` — list of BigQuery row objects for the current batch
- `is_first_batch` — boolean; when `True`, the local CSV file is opened in write mode (overwrite), otherwise in append mode

**Returns**: list of dicts parsed from the CSV response, one dict per geocoded address

**Side effects**:
- Appends/writes to `extracted_addresses.csv` on disk (acts as a local audit log)
- Makes an HTTP POST to `API_URL` (`https://data.geopf.fr/geocodage/search/csv`)

---

### `run_pipeline(table_name, batch_size)`

**File**: `main.py:63`

Orchestrates the full pipeline run.

**Parameters**:
- `table_name` — destination table name in DuckDB (default: `geocoded_addresses`)
- `batch_size` — number of rows fetched from BigQuery per page (default: `10`)

**Steps**:

1. **Query BigQuery** — runs the SQL defined in `const.QUERY` against the `passculture-data-prod` GCP project, fetching results paginated by `batch_size`
2. **Create dlt pipeline** — targets DuckDB, dataset `geocoding_data`
3. **Iterate over pages** — for each page of BigQuery results:
   - Calls `call_geocoding_search_batch_api()` with the current page's rows
   - Sets `write_disposition = "replace"` for the first batch, `"append"` for subsequent ones
   - Defines an inline dlt resource and runs it through the pipeline
4. **Logs summary** — total rows processed once all pages are consumed

---

## Data Flow

```
BigQuery (passculture-data-prod)
    │
    │  SQL query (const.QUERY) — returns user_id + user_full_address
    │  paginated by batch_size
    ▼
Batch of rows (list of BigQuery Row objects)
    │
    ├──► extracted_addresses.csv  (local audit log, written batch by batch)
    │
    │  HTTP POST multipart/form-data
    ▼
Geopf API  (https://data.geopf.fr/geocodage/search/csv)
    │
    │  CSV response with original columns + geocoding result columns
    ▼
dlt pipeline
    │
    ▼
DuckDB  →  geocoding_data.geocoded_addresses
```

---

## Source Data (BigQuery Query)

The SQL in `const.QUERY` builds the list of addresses to geocode:

1. **`profile_completion_check`** — extracts address fields from the fraud check table where users completed their profile (`PROFILE_COMPLETION / OK`)
2. **`profile_completion`** — concatenates address + postal code + city into a single `user_full_address` string, filtering out rows with missing address fields
3. **`applicative_database_user`** — same address concatenation from the main user table
4. **`all_users`** — `UNION ALL` of both sources above
5. **`user_candidates`** — deduplicates by `user_id`, keeping the most recently modified address
6. **`user_location_update`** — retrieves the last geocoding calculation date per user (currently unused in the final `WHERE` clause, which is commented out)

**Final `SELECT`**: returns `user_id` and `user_full_address` for up to 25 users (controlled by `LIMIT 25`).

---

## Configuration (`const.py`)

| Constant | Value |
|---|---|
| `API_URL` | `https://data.geopf.fr/geocodage/search/csv` |
| `GCP_PROJECT` | `passculture-data-prod` |
| `DATASET_NAME` | `geocoding_data` |
| `DEFAULT_DESTINATION_TABLE_NAME` | `geocoded_addresses` |

---

## Output

| Output | Description |
|---|---|
| `geopf_geocoding.duckdb` | DuckDB database with geocoded results |
| `extracted_addresses.csv` | Local copy of every address sent to the API |

The DuckDB table `geocoding_data.geocoded_addresses` contains the original columns (`user_id`, `user_full_address`) plus all columns returned by the Geopf API, including `result_label`, `result_score`, `result_type`, `latitude`, `longitude`, and additional metadata.

---

## Notable Behaviour

- **Write disposition per batch**: the first batch uses `"replace"` (drops and recreates the table), subsequent batches use `"append"`. This means a fresh run always starts with a clean table.
- **Inline dlt resource**: the `@dlt.resource` decorated function is defined inside the loop. Each iteration creates a new resource bound to the current batch's data.
- **`page_number` tracking**: `results.page_number` is read after iterating the page, so the logged batch number reflects the page that was just processed.
