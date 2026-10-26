import datetime
import logging

from google.cloud import bigquery
from utils import ENV_SHORT_NAME, GCP_PROJECT

# Tables stale for this long are moved to sandbox. A dbt model still in use is
# rebuilt by its next run, so there is no need to tell dbt tables apart.
MOVE_AFTER_DAYS = 60
# The alert announces the move this many days before it happens.
NOTICE_DAYS = 14
SANDBOX_EXPIRATION_DAYS = 90
# A table read in this window is still used, so it is never moved.
USAGE_LOOKBACK_DAYS = 30
# Stale ingestion tables mean a broken source feed, and snapshots hold history
# that cannot be rebuilt.
PROTECTED_DATASET_PREFIXES = ("raw_", "backend_", "snapshot_")

SANDBOX_DATASET = f"sandbox_{ENV_SHORT_NAME}"
MOVE_LOG_TABLE = f"{GCP_PROJECT}.raw_{ENV_SHORT_NAME}.stale_tables_moved_log"
MOVE_LOG_SCHEMA = [
    bigquery.SchemaField("moved_at", "TIMESTAMP"),
    bigquery.SchemaField("source_table", "STRING"),
    bigquery.SchemaField("sandbox_table", "STRING"),
    bigquery.SchemaField("last_modified", "DATE"),
    bigquery.SchemaField("last_writer", "STRING"),
]


def get_used_tables(client, tables):
    query = f"""
        SELECT DISTINCT referenced_table
        FROM `{GCP_PROJECT}.int_gcp_{ENV_SHORT_NAME}.bigquery_cost`,
            UNNEST(SPLIT(referenced_tables, ",")) AS referenced_table
        WHERE start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {USAGE_LOOKBACK_DAYS} DAY)
            AND referenced_table IN UNNEST(@tables)
    """
    return set(_query_column(client, query, tables))


def get_tables_read_by_views(client, tables):
    # Substring match: a false positive only keeps a table, never moves one.
    query = f"""
        SELECT table_name
        FROM UNNEST(@tables) AS table_name
        WHERE EXISTS (
            SELECT 1
            FROM `{GCP_PROJECT}.region-europe-west1`.INFORMATION_SCHEMA.VIEWS
            WHERE STRPOS(LOWER(view_definition), LOWER(table_name)) > 0
        )
    """
    return set(_query_column(client, query, tables))


def _query_column(client, query, tables):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tables", "STRING", tables)]
    )
    return [row[0] for row in client.query(query, job_config=job_config).result()]


def log_move(client, table, sandbox_table):
    # Labelled "never" so this alert does not flag its own log between moves.
    log_table = bigquery.Table(MOVE_LOG_TABLE, schema=MOVE_LOG_SCHEMA)
    log_table.labels = {"schedule": "never"}
    client.create_table(log_table, exists_ok=True)
    client.load_table_from_json(
        [
            {
                "moved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "source_table": table["table"],
                "sandbox_table": sandbox_table,
                "last_modified": table["last_modified"],
                "last_writer": table.get("last_writer"),
            }
        ],
        MOVE_LOG_TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", schema=MOVE_LOG_SCHEMA
        ),
    ).result()


def move_to_sandbox(client, table):
    source = f"{GCP_PROJECT}.{table['table']}"
    sandbox_table = f"{SANDBOX_DATASET}.{table['table'].replace('.', '__')}"
    destination = f"{GCP_PROJECT}.{sandbox_table}"

    client.copy_table(
        source,
        destination,
        job_config=bigquery.CopyJobConfig(write_disposition="WRITE_EMPTY"),
    ).result()

    copy = client.get_table(destination)
    copy.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        days=SANDBOX_EXPIRATION_DAYS
    )
    copy.labels = {**copy.labels, "moved_from": table["table"].split(".")[0]}
    client.update_table(copy, ["expires", "labels"])

    log_move(client, table, sandbox_table)
    # The source is dropped only once its sandbox copy exists and is logged.
    client.delete_table(source)
    return sandbox_table


def get_sandbox_status(table, dry_run, client, used_tables, view_tables):
    """Decide what happens to a stale table, and move it if it is time."""
    move_on = datetime.date.fromisoformat(table["last_modified"]) + datetime.timedelta(
        days=MOVE_AFTER_DAYS
    )
    if table["days_since_update"] < MOVE_AFTER_DAYS:
        return f"moves to {SANDBOX_DATASET} on {move_on}"
    if table["table"] in used_tables:
        return f"kept, read in the last {USAGE_LOOKBACK_DAYS}d"
    if table["table"] in view_tables:
        return "kept, read by a view"
    if client.get_table(f"{GCP_PROJECT}.{table['table']}").table_type != "TABLE":
        return "kept, not a table"
    if dry_run:
        return f"would move to {SANDBOX_DATASET} (dry run)"
    try:
        return f"moved to {move_to_sandbox(client, table)}"
    except Exception as e:
        logging.exception(f"Could not move {table['table']} to sandbox")
        return f"move failed: {type(e).__name__}"


def sandbox_stale_tables(warning_tables, dry_run):
    """
    Adds a "sandbox" status to stale tables close to the move threshold,
    and moves the ones past it to the sandbox dataset unless dry_run.
    """
    candidates = [
        table
        for table in warning_tables
        if not table["table"].startswith(PROTECTED_DATASET_PREFIXES)
        and table["days_since_update"] >= MOVE_AFTER_DAYS - NOTICE_DAYS
    ]
    if not candidates:
        return

    client = bigquery.Client(project=GCP_PROJECT)
    due = [t["table"] for t in candidates if t["days_since_update"] >= MOVE_AFTER_DAYS]
    used_tables = get_used_tables(client, due) if due else set()
    view_tables = get_tables_read_by_views(client, due) if due else set()

    for table in candidates:
        table["sandbox"] = get_sandbox_status(
            table, dry_run, client, used_tables, view_tables
        )
        logging.info(f"{table['table']}: {table['sandbox']}")
