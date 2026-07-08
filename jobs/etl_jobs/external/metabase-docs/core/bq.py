"""BigQuery IO for the dashboard-docs enrichment job."""

import logging
import re
from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from core.utils import (
    ASSET_CATALOG_TABLE,
    BIGQUERY_INT_METABASE_DATASET,
    BIGQUERY_RAW_DATASET,
    DASHBOARD_CARD_TABLE,
    DASHBOARD_DOCS_TABLE,
    GCP_PROJECT,
    NOTION_DOCS_TABLE,
    QUERY_EXECUTION_TABLE,
    QUERY_TABLE,
)

logger = logging.getLogger(__name__)

BQ_SCHEMA = [
    bigquery.SchemaField("page_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("notion_url", "STRING"),
    bigquery.SchemaField("dashboard_id", "INTEGER"),
    bigquery.SchemaField("dashboard_url", "STRING"),
    bigquery.SchemaField("summary", "STRING"),
    bigquery.SchemaField("questions_answered", "STRING", mode="REPEATED"),
    bigquery.SchemaField("key_metrics", "STRING", mode="REPEATED"),
    bigquery.SchemaField("dimensions", "STRING", mode="REPEATED"),
    bigquery.SchemaField("caveats", "STRING", mode="REPEATED"),
    bigquery.SchemaField("related_concepts", "STRING", mode="REPEATED"),
    bigquery.SchemaField("audience", "STRING"),
    bigquery.SchemaField("definition_alignment", "STRING"),
    bigquery.SchemaField("confidence", "STRING"),
    bigquery.SchemaField("dashboard_exists", "BOOLEAN"),
    bigquery.SchemaField("doc_is_stale", "BOOLEAN"),
    bigquery.SchemaField("source_hash", "STRING"),
    bigquery.SchemaField("prompt_version", "STRING"),
    bigquery.SchemaField("execution_date", "TIMESTAMP"),
]

_NATIVE_SQL_RE = re.compile(r"\s+")


def _raw(table: str) -> str:
    return f"`{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table}`"


def _int(table: str) -> str:
    return f"`{GCP_PROJECT}.{BIGQUERY_INT_METABASE_DATASET}.{table}`"


def read_notion_docs(client: bigquery.Client) -> list:
    """One row per doc from the latest partition (docs deleted from Notion drop out).

    The notion export is a full snapshot per run, so the latest partition is exactly the
    set of docs that currently exist in Notion.
    """
    q = f"""
        SELECT page_id, title, notion_url, dashboard_id, dashboard_url, body_md, last_edited
        FROM {_raw(NOTION_DOCS_TABLE)}
        QUALIFY RANK() OVER (ORDER BY execution_date DESC) = 1
    """
    rows = [dict(r) for r in client.query(q).result()]
    logger.info("Read %s docs from %s", len(rows), NOTION_DOCS_TABLE)
    return rows


def read_existing_docs(client: bigquery.Client) -> dict:
    """page_id -> previous enriched row (for the incremental source_hash cache).

    Empty on the first run (table does not exist yet).
    """
    q = f"""
        SELECT * EXCEPT(execution_date)
        FROM {_int(DASHBOARD_DOCS_TABLE)}
        QUALIFY RANK() OVER (ORDER BY execution_date DESC) = 1
    """
    try:
        rows = {r["page_id"]: dict(r) for r in client.query(q).result()}
    except NotFound:
        logger.info("%s does not exist yet — first run.", DASHBOARD_DOCS_TABLE)
        return {}
    logger.info("Loaded %s previously-enriched docs (incremental cache)", len(rows))
    return rows


def read_asset_catalog_dashboards(client: bigquery.Client) -> dict:
    """dashboard_id -> asset_catalog dashboard row (classification + member cards)."""
    q = f"""
        SELECT
            asset_id AS dashboard_id, asset_name, description, dashboard_description,
            dashboard_markdown, member_cards, squad, tier, certified, is_archived
        FROM {_int(ASSET_CATALOG_TABLE)}
        WHERE asset_kind = 'dashboard'
    """
    rows = {int(r["dashboard_id"]): dict(r) for r in client.query(q).result()}
    logger.info("Read %s dashboards from %s", len(rows), ASSET_CATALOG_TABLE)
    return rows


def read_dashboard_card_ids(client: bigquery.Client) -> dict:
    """dashboard_id -> [card_id] (ids needed to fetch card SQL; names live in asset_catalog)."""
    q = f"""
        SELECT dashboard_id, ARRAY_AGG(DISTINCT card_id IGNORE NULLS) AS card_ids
        FROM {_raw(DASHBOARD_CARD_TABLE)}
        WHERE card_id IS NOT NULL
        GROUP BY dashboard_id
    """
    return {
        int(r["dashboard_id"]): [int(c) for c in r["card_ids"]]
        for r in client.query(q).result()
    }


def read_dashboard_card_last_change(client: bigquery.Client) -> dict:
    """dashboard_id -> latest updated_at across its member cards (staleness heuristic)."""
    q = f"""
        SELECT dc.dashboard_id, MAX(rc.updated_at) AS last_card_change
        FROM {_raw(DASHBOARD_CARD_TABLE)} AS dc
        JOIN {_raw("metabase_report_card")} AS rc ON dc.card_id = rc.id
        GROUP BY dc.dashboard_id
    """
    return {
        int(r["dashboard_id"]): r["last_card_change"] for r in client.query(q).result()
    }


def _extract_native_sql(blob) -> str:
    """Pull the compiled native SQL out of a Metabase query_raw JSON blob."""
    import json

    try:
        obj = json.loads(blob)
    except (TypeError, ValueError):
        return ""
    if not isinstance(obj, dict):
        return ""
    for st in obj.get("stages") or []:
        if (
            isinstance(st, dict)
            and isinstance(st.get("native"), str)
            and st["native"].strip()
        ):
            return st["native"]
    nat = obj.get("native")
    if isinstance(nat, dict) and isinstance(nat.get("query"), str):
        return nat["query"]
    return nat if isinstance(nat, str) else ""


def card_sql_snippet(
    client: bigquery.Client, card_id: int, limit_chars: int = 600
) -> str:
    """Most recent successful compiled SQL for a card, whitespace-collapsed and truncated."""
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("cid", "INT64", int(card_id))]
    )
    q = f"""
        SELECT q.query_raw
        FROM {_raw(QUERY_EXECUTION_TABLE)} e
        JOIN {_raw(QUERY_TABLE)} q ON e.hash = q.query_hash
        WHERE e.card_id = @cid AND e.error IS NULL AND q.query_raw IS NOT NULL
        ORDER BY e.execution_date DESC LIMIT 1
    """
    for r in client.query(q, job_config=cfg).result():
        sql = _extract_native_sql(r["query_raw"])
        if sql:
            return _NATIVE_SQL_RE.sub(" ", sql)[:limit_chars]
    return ""


def save_docs(client: bigquery.Client, rows: list, execution_date: datetime):
    """Load the enriched rows into int_metabase_<env>.dashboard_documentation."""
    if not rows:
        logger.warning("No rows to save — leaving %s untouched.", DASHBOARD_DOCS_TABLE)
        return
    yyyymmdd = execution_date.strftime("%Y%m%d")
    table_id = f"{GCP_PROJECT}.{BIGQUERY_INT_METABASE_DATASET}.{DASHBOARD_DOCS_TABLE}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
        ),
    )
    logger.info("Loading %s rows → %s", len(rows), table_id)
    client.load_table_from_json(rows, table_id, job_config=job_config).result()
