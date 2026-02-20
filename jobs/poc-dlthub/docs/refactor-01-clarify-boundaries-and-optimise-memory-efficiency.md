# Plan: Refactor main.py with clear boundaries and efficient data representation

## Context

Currently `call_geocoding_search_batch_api` receives BigQuery `Row` objects, converts them to dicts internally, and builds CSV with `csv.DictWriter`, then returns `list[dict]` for dlt. This:
- Couples geocoding logic to BigQuery types
- Creates N dicts with M repeated key strings per batch (input and output)
- Holds up to 5 simultaneous copies of the batch data in memory

dlt natively supports `pyarrow.Table` as a yield type. When a resource yields Arrow tables, dlt routes them through its `ArrowExtractor` which writes Parquet directly — and DuckDB loads Parquet natively. No Python dict objects, no row-by-row serialization. The entire data path stays in C++/columnar format.

## Memory analysis: current vs proposed

### Current — ~5 copies per batch

| Stage | Copy | Type |
|-------|------|------|
| `list(page)` | BigQuery Row objects | input #1 |
| `csv_buffer` (StringIO) | CSV as Python string | input #2 |
| `csv_buffer.encode()` | CSV as bytes | input #3 |
| `response.text` | API response string | output #1 |
| `list(csv_reader)` | N dicts, M keys each | output #2 |

### Proposed — ~2 copies per batch (1 input + 1 output)

| Stage | Copy | Type |
|-------|------|------|
| `BytesIO` via `TextIOWrapper` | CSV as bytes (streamed from generator) | input #1 |
| `response.content` → `pyarrow.csv.read_csv()` | Arrow table in C++ memory | output #1 |

- **Input**: generator streams BigQuery rows → tuples → CSV bytes. No intermediate list, no string→bytes conversion.
- **Output**: `pyarrow.csv.read_csv(io.BytesIO(response.content))` parses CSV directly into columnar Arrow buffers in C++. No Python dict objects created. `response.content` (bytes) is used instead of `response.text` (string), avoiding a bytes→string decode that would just be re-encoded by a Python CSV parser.

## Changes

### 1. Add pyarrow dependency — `pyproject.toml`

Change:
```
"dlt[duckdb]>=1.21.0",
```
To:
```
"dlt[duckdb,parquet]>=1.21.0",
```

This pulls in pyarrow (required for `pyarrow.csv` and dlt's Arrow route).

### 2. Extract columns at the BigQuery boundary — `main.py`

In `run_pipeline`, after `results = query_job.result(...)`, extract columns from the schema once:

```python
columns = [field.name for field in results.schema]
```

In the page loop, use a **generator** (not a list) to convert rows lazily:

```python
for page in results.pages:
    rows = (tuple(row.values()) for row in page)
    logger.info(f"Processing batch with {page.num_items} rows")
    yield call_geocoding_search_batch_api(columns, rows)
```

Note: `yield` (not `yield from`) — we yield one Arrow table per batch, not individual rows.

### 3. Refactor `call_geocoding_search_batch_api` — `main.py`

New signature:

```python
def call_geocoding_search_batch_api(
    columns: list[str], rows: Iterable[tuple]
) -> pa.Table:
```

New body:

```python
import pyarrow.csv as pa_csv

# Build request CSV directly as bytes
bytes_buffer = io.BytesIO()
text_wrapper = io.TextIOWrapper(bytes_buffer, encoding="utf-8", newline="")
writer = csv.writer(text_wrapper)
writer.writerow(columns)
writer.writerows(rows)
text_wrapper.flush()
bytes_buffer.seek(0)

# Call API
files = {"data": ("addresses.csv", bytes_buffer, "text/csv")}
response = _rest_client.session.post(API_URL, files=files)
response.raise_for_status()

# Parse response CSV directly into Arrow table (C++ parsing, no Python dicts)
return pa_csv.read_csv(io.BytesIO(response.content))
```

### 4. Clean up imports — `main.py`

- Remove: `Dict` from `typing`
- Add: `Iterable` from `collections.abc`
- Add: `import pyarrow.csv as pa_csv`

## Testability

`call_geocoding_search_batch_api` takes `list[str]` + any `Iterable[tuple]`, returns a `pyarrow.Table`. A test:

```python
table = call_geocoding_search_batch_api(
    ["user_id", "user_full_address"],
    [("1", "8 bd du port 38000 Grenoble")],
)
assert "latitude" in table.column_names
assert table.num_rows == 1
```

No BigQuery imports, no mocking of Row objects. Arrow tables have rich assertion support (column names, types, num_rows).

## Files modified

- `pyproject.toml` — add `parquet` extra to dlt
- `main.py` — refactor as described above

## Verification

```bash
uv sync  # install pyarrow
uv run main.py
```

Pipeline should complete with "Pipeline completed successfully! 25 total rows".
