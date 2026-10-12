# Plan: Use BigQuery Storage API via `to_arrow_iterable()`

## Context

The current pipeline reads BigQuery results via the REST-based `results.pages` iterator, which returns Python `Row` objects that are then manually converted to CSV via Python's `csv` module. The goal is to switch to the BigQuery Storage API using `to_arrow_iterable()`, which returns `pyarrow.RecordBatch` objects via gRPC — faster and more memory-efficient. The `call_geocoding_search_batch_api` function should also be updated to accept a `RecordBatch` directly and use PyArrow's native CSV writer instead of Python's `csv` module.

## Critical File

- `main.py` — only file that needs changes

## Changes in `main.py`

### 1. Update imports

- Remove `import csv` (no longer needed — PyArrow handles CSV writing)
- Remove `from collections.abc import Iterable` (no longer needed)
- Add `from pyarrow import RecordBatch` (or reuse existing pyarrow imports)

### 2. Update `call_geocoding_search_batch_api`

**Before:**
```python
def call_geocoding_search_batch_api(columns: list[str], rows: Iterable[tuple]) -> Table:
    bytes_buffer = io.BytesIO()
    text_wrapper = io.TextIOWrapper(bytes_buffer, encoding="utf-8", newline="")
    writer = csv.writer(text_wrapper)
    writer.writerow(columns)
    writer.writerows(rows)
    text_wrapper.flush()
    bytes_buffer.seek(0)
    ...
```

**After:**
```python
def call_geocoding_search_batch_api(batch: RecordBatch) -> Table:
    bytes_buffer = io.BytesIO()
    pa_csv.write_csv(batch, bytes_buffer)
    bytes_buffer.seek(0)
    ...
```

- Signature changes from `(columns, rows)` to `(batch: RecordBatch)`
- CSV creation uses `pyarrow.csv.write_csv()` (C++ implementation) instead of Python's `csv.writer`
- Removes `TextIOWrapper` — writes directly to `BytesIO`

### 3. Update `run_pipeline`

**Before:**
```python
results = query_job.result(page_size=batch_size)
columns = [field.name for field in results.schema]
...
    for page in results.pages:
        rows = (tuple(row.values()) for row in page)
        logger.info(f"Processing batch with {page.num_items} rows")
        yield call_geocoding_search_batch_api(columns, rows)
```

**After:**
```python
results = query_job.result(page_size=batch_size)
...
    for arrow_batch in results.to_arrow_iterable():
        logger.info(f"Processing batch with {arrow_batch.num_rows} rows")
        yield call_geocoding_search_batch_api(arrow_batch)
```

- `page_size=batch_size` is kept — it controls how many rows each batch contains
- `results.to_arrow_iterable()` replaces `results.pages` — yields `RecordBatch` objects via the Storage API
- `columns` extraction is removed — no longer needed since `RecordBatch` carries its own schema
- Each `RecordBatch` is passed directly to the updated API function

## Verification

```bash
uv run python main.py
```

Expected: pipeline runs, Storage API is used for BQ reads (gRPC), geocoding batches are sent to Geopf API using PyArrow CSV serialization, results land in DuckDB.
