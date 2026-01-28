from datetime import datetime, timedelta, timezone
from typing import Literal

import pandas as pd
from google.api_core import exceptions
from google.cloud import bigquery
from jinja2 import Template

from tools.config import GCP_PROJECT_ID
from utils.logging import logging


def load_data(
    gcp_project: str,
    input_dataset_name: str,
    input_table_name: str,
    max_rows_to_process: int,
) -> pd.DataFrame:
    # If max_rows_to_process is -1, we will process all data.
    limit = f" LIMIT {max_rows_to_process} " if max_rows_to_process > 0 else ""
    return pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.{input_dataset_name}.{input_table_name}` {limit} "
    )


def create_or_clear_tmp_table(client: bigquery.Client, table_name: str) -> None:
    """
    Ensures a BigQuery table exists.
    If the table does not exist, it is created empty.
    If it exists, it is cleared (DELETE WHERE TRUE).
    """
    try:
        # Check existence
        client.get_table(table_name)

        # If table exists, clear it
        client.query(f"DELETE FROM `{table_name}` WHERE TRUE").result()
        logging.info(f"Cleared tmp table {table_name}")

    except exceptions.NotFound:
        # If table does not exist, create empty table
        logging.info(f"Tmp table {table_name} does not exist — creating it.")
        client.query(
            f"CREATE TABLE `{table_name}` AS SELECT * FROM UNNEST([])"
        ).result()
        logging.info(f"Created empty tmp table {table_name}")


def write_to_tmp(
    df: pd.DataFrame,
    tmp_table: str,
    if_exists: Literal["fail", "replace", "append"] = "append",
    project_id: str = GCP_PROJECT_ID,
):
    """
    Writes a DataFrame to a tmp table in BigQuery (replaces existing staging table)
    """
    df.to_gbq(
        tmp_table,
        project_id=project_id,
        if_exists=if_exists,
    )
    logging.info(
        f"Wrote {len(df)} rows to tmp table {tmp_table} (if_exists={if_exists})"
    )


def merge_upsert(
    tmp_table: str,
    main_table: str,
    primary_key: str,
    project_id: str,
    ttl_hours: int = 24,
    date_columns: list[str] | None = None,
    *,
    nullify_deprecated_columns: bool = False,
):
    """
    Performs a MERGE in BigQuery to upsert rows from tmp_table into main_table

    Parameters:
    - tmp_table: source table with new rows/updates
    - main_table: target table to upsert into
    - primary_key: column name used as key for matching rows
    - date_columns: list of columns to be cast to DATE
    - project_id: GCP project ID
    - ttl_hours: expiration time for tmp_table in hours
    - nullify_deprecated_columns: if True, target columns not in tmp_table are set to NULL
    """

    client = bigquery.Client(project=project_id)

    tmp_schema = client.get_table(tmp_table).schema
    main_schema = client.get_table(main_table).schema

    tmp_cols = {col.name for col in tmp_schema}
    main_cols = {col.name for col in main_schema}
    date_columns = date_columns or []

    # --- Build UPDATE assignments ---
    update_set_list = []
    for col in main_cols:
        if col == primary_key:
            continue
        if col in tmp_cols:
            if col in date_columns:
                update_set_list.append(f"T.{col} = CAST(S.{col} AS DATE)")
            else:
                update_set_list.append(f"T.{col} = S.{col}")
        elif nullify_deprecated_columns:
            update_set_list.append(f"T.{col} = NULL")

    # --- Build INSERT column/value lists ---
    insert_columns = [c.name for c in tmp_schema]
    insert_values = [
        f"CAST(S.{col} AS DATE)" if col in date_columns else f"S.{col}"
        for col in insert_columns
    ]

    # --- Inline Jinja template ---
    jinja_sql = """
    MERGE `{{ main_table }}` T
    USING `{{ tmp_table }}` S
    ON T.{{ primary_key }} = S.{{ primary_key }}

    WHEN MATCHED THEN
      UPDATE SET
    {% for clause in update_set_list %}
        {{ clause }}{% if not loop.last %},{% endif %}
    {% endfor %}

    WHEN NOT MATCHED THEN
      INSERT (
        {{ insert_columns | join(', ') }}
      )
      VALUES (
        {{ insert_values | join(', ') }}
      )
    """

    # Render template
    template = Template(jinja_sql)
    merge_query = template.render(
        main_table=main_table,
        tmp_table=tmp_table,
        primary_key=primary_key,
        update_set_list=update_set_list,
        insert_columns=insert_columns,
        insert_values=insert_values,
    )

    # --- Execute query ---
    client.query(merge_query).result()
    logging.info(f"Merged tmp table {tmp_table} into main table {main_table}")

    # --- Set expiration on tmp table ---
    tmp_table_obj = client.get_table(tmp_table)
    tmp_table_obj.expires = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
    client.update_table(tmp_table_obj, ["expires"])
