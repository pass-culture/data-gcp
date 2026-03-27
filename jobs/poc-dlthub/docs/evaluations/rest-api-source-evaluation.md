# Evaluation: Is `main.py` using the dlt REST API verified source correctly?

## Short answer

**No** — the code does not use the dlt REST API verified source (`rest_api_source` / `RESTAPIConfig`) at all. However, this is **the right decision** for the Geopf geocoding API, because that source is designed for JSON-based REST APIs and would be a poor fit here.

---

## What the dlt REST API source is

The [REST API verified source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api) provides a **declarative, config-driven** way to extract data from REST APIs. You describe the API in a Python dictionary and dlt handles pagination, authentication, parent-child relationships, and incremental loading automatically.

A typical usage looks like this:

```python
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com/",
        "auth": {"token": dlt.secrets.value},
    },
    "resources": [
        {
            "name": "users",
            "endpoint": {
                "path": "users",
                "params": {"per_page": 100},
                "paginator": {"type": "json_link", "next_url_path": "next"},
            },
        }
    ],
}

pipeline = dlt.pipeline(...)
pipeline.run(rest_api_resources(config))
```

### Key assumptions baked into the source

| Assumption | Geopf API reality |
|---|---|
| Responses are **JSON** | Responses are **CSV** |
| Requests use query params or a JSON body | Requests use **multipart file upload** (`files=`) |
| The API is read-only (GET, or POST with filters) | The API transforms an **input CSV** into an output CSV |
| Pagination is response-driven (next links, offsets…) | Pagination is **caller-driven** (BigQuery page iteration) |

None of the built-in mechanisms (paginators, `data_selector`, `json`/`data` body params) can handle the Geopf multipart CSV upload + CSV response pattern without heavy customisation.

---

## What the code does instead

`main.py` uses a **manual approach** with a custom `@dlt.resource`:

1. Iterates BigQuery result pages (pagination is on the **source** side, not the API side).
2. Converts each page to CSV in memory.
3. POSTs the CSV as a multipart file to the Geopf API via `requests.post(url, files=...)`.
4. Parses the CSV response with `csv.DictReader`.
5. Yields the parsed dicts through an inline `@dlt.resource` into the dlt pipeline.

This is the pattern recommended by the dlt documentation itself for non-JSON APIs.

---

## Could the REST API source be made to work?

In theory, yes — with significant effort:

- **Custom response action** to parse CSV into JSON: you would need a callable that rewrites `response._content` from CSV to JSON.
- **Custom request hook or paginator** to build multipart form-data uploads — the `data` endpoint parameter only supports form-encoded key-value pairs, not `files=`.
- **External pagination control** would still need to be handled outside the source since it comes from BigQuery, not the API.

The result would be harder to read and maintain than the current manual approach.

---

## Possible improvements (using dlt, but not the REST API source)

While the current approach is fundamentally sound, a few dlt features could improve it:

### 1. Use `dlt.sources.helpers.rest_client.RESTClient` for retries and timeouts

```python
from dlt.sources.helpers.rest_client import RESTClient

client = RESTClient(base_url="https://data.geopf.fr/geocodage")
# Gives you automatic retries, configurable timeouts, and backoff
```

This is the **lower-level** layer of the REST API source. It would not replace the CSV handling logic, but it would add resilience to the HTTP calls without the overhead of the full declarative config.

### 2. Define the resource outside the loop

The current code defines a new `@dlt.resource` decorated function **inside** the batch loop. This works, but a cleaner pattern is to define a single generator-based resource and use `pipeline.run()` once:

```python
@dlt.resource(write_disposition="replace", name=table_name)
def geocoded_addresses():
    for page in results.pages:
        rows_batch = list(page)
        yield from call_geocoding_search_batch_api(rows_batch, ...)

pipeline.run(geocoded_addresses())
```

This lets dlt manage write disposition and batching internally, and avoids recreating the resource on every iteration.

### 3. Drop the `is_first_batch` / replace-then-append pattern

With a single resource (as above), `write_disposition="replace"` would apply once at the start of the run, and all yielded records would go into the same load. No need to switch between `replace` and `append` manually.

---

## Verdict

| Criterion | Assessment |
|---|---|
| Uses the REST API verified source? | No |
| Should it? | **No** — the Geopf CSV-in/CSV-out API is not a good fit |
| Is the current manual approach valid? | **Yes** — it follows the dlt-recommended pattern for non-JSON APIs |
| Room for improvement? | Yes — single resource definition, `RESTClient` for retries, simpler write disposition |
