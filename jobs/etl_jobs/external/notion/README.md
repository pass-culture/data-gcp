# notion — import Notion dashboard-documentation into the raw layer

Exports the Notion **dashboard-docs** database (human-written documentation of Metabase
dashboards) into `raw_<env>.notion_dashboard_docs`, one row per doc, with the Metabase
dashboard/question deep link auto-detected.

## Run

```bash
# env (also injected by the DAG): GCP_PROJECT, ENV_SHORT_NAME

uv run main.py export    # export → raw_<env>.notion_dashboard_docs
```

## Config

| variable | purpose | default |
|---|---|---|
| `GCP_PROJECT` | BigQuery + Secret Manager project | — |
| `ENV_SHORT_NAME` | `dev` / `stg` / `prod`; suffixes datasets + secrets | — |
| `NOTION_TOKEN` | integration token override | secret `notion_api_key_<env>` |
| `NOTION_DB_ID` | database id override | secret `notion_dashboard_database_id_<env>` |
| `NOTION_DB_SECRET` | alternate Secret Manager key for the DB id | `notion_dashboard_database_id_<env>` |
| `NOTION_VERSION` | Notion API version | `2022-06-28` |

To export a different Notion DB (e.g. the table-docs DB), set
`NOTION_DB_SECRET=notion_documentation_database_id_<env>`.

## Output — `raw_<env>.notion_dashboard_docs`

Day-partitioned on `execution_date`, `WRITE_TRUNCATE` per run.

| column | type | content |
|---|---|---|
| `page_id` | STRING | Notion page id (natural key) |
| `title` | STRING | doc title |
| `notion_url` | STRING | canonical Notion URL |
| `dashboard_id` | INTEGER (nullable) | Metabase dashboard/question id, else null |
| `dashboard_url` | STRING (nullable) | matched Metabase URL |
| `properties` | STRING | all Notion properties, generically rendered, as JSON |
| `body_md` | STRING | page body rendered to markdown |
| `last_edited` | TIMESTAMP | Notion `last_edited_time` |
| `execution_date` | TIMESTAMP | run timestamp (partition field) |
