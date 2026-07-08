# metabase-docs

Enrich + reconcile the imported Notion dashboard docs into a structured, queryable
"dashboard ontology" table.

**PR 2 of 2** — depends on the `notion` import job (`raw_<env>.notion_dashboard_docs`) and on the
daily Metabase sync (`metabase-governance` + dbt → `int_metabase_<env>.asset_catalog`).

## What it does

For each doc in the latest partition of `raw_<env>.notion_dashboard_docs`:

1. **Grounds** it in the dashboard's real cards — `int_metabase_<env>.asset_catalog`
   (`member_cards`, classification, in-Metabase markdown) plus a few compiled card SQL snippets from
   `raw_<env>.metabase_query(_execution)`. Lineage/classification truth is `asset_catalog`, **not**
   `dbt_exposures` (which is downstream of the catalog).
2. **LLM-enriches** it via Vertex AI Gemini into a structured spec (summary, questions answered, key
   metrics, dimensions, caveats, related concepts, audience, `definition_alignment`, `confidence`).
   The output is forced + validated against `core.prompts.DashboardSpec` via pydantic-ai, and the
   untrusted doc/card content is fenced (`<document>` / `<cards>`) so it is summarized, not obeyed.
3. **Reconciles** it structurally (no LLM): `dashboard_exists` (live, non-archived) and `doc_is_stale`
   (doc older than the dashboard's latest card change).
4. **Lands** one row per documented dashboard in `int_metabase_<env>.dashboard_documentation`
   (day-partitioned, `WRITE_TRUNCATE`).

Enrichment is **incremental**: a `source_hash` (`sha256(prompt_version + body)`) is stored per row;
unchanged docs are carried forward without an LLM call.

## Run

```bash
uv run main.py enrich           # incremental
uv run main.py enrich --force   # re-enrich every doc
```

## Config (env)

| var | meaning |
|---|---|
| `GCP_PROJECT` | BigQuery + Vertex project |
| `ENV_SHORT_NAME` | `dev` / `stg` / `prod` (dataset suffix) |
| `VERTEX_LOCATION` | Vertex region (default `europe-west1`) |
| `VERTEX_MODEL` | Gemini model (default `gemini-2.5-flash`) |
| `SQL_SNIPPET_CARDS` | cards to include SQL for (default `4`) |
