from time import time

import pandas as pd
from google.cloud import bigquery

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


def write_to_tmp(
    df: pd.DataFrame,
    tmp_table: str,
    project_id: str = GCP_PROJECT_ID,
):
    """
    Writes a DataFrame to a tmp table in BigQuery (replaces existing staging table)
    """
    df.to_gbq(
        tmp_table,
        project_id=project_id,
        if_exists="append",
    )
    logging.info(f"Wrote {len(df)} rows to tmp table {tmp_table}")


def generate_update_set(
    target_table: str, exclude_columns: list, project_id: str
) -> str:
    client = bigquery.Client(project=project_id)
    table = client.get_table(target_table)
    columns = [
        f"{col.name} = S.{col.name}"
        for col in table.schema
        if col.name not in exclude_columns
    ]
    return ",\n  ".join(columns)


def merge_upsert(
    tmp_table: str,
    main_table: str,
    primary_key: str,
    project_id: str,
    ttl_hours: int = 24,
    *,
    nullify_deprecated_columns: bool = False,
):
    """
    Performs a MERGE in BigQuery to upsert rows from tmp_table into main_table

    Parameters:
    - tmp_table: source table with new rows/updates
    - main_table: target table to upsert into
    - primary_key: column name used as key for matching rows
    - project_id: GCP project ID
    - ttl_hours: expiration time for tmp_table in hours
    - nullify_deprecated_columns: if True, target columns not in tmp_table are set to NULL
    """
    client = bigquery.Client(project=project_id)

    tmp_schema = client.get_table(tmp_table).schema
    main_schema = client.get_table(main_table).schema

    tmp_cols = {col.name for col in tmp_schema}
    main_cols = {col.name for col in main_schema}
    update_set_list = []
    for col in main_cols:
        if col == primary_key:
            continue
        if col in tmp_cols:
            update_set_list.append(f"T.{col} = S.{col}")
        elif nullify_deprecated_columns:
            update_set_list.append(f"T.{col} = NULL")

    update_set = ",\n    ".join(update_set_list)

    insert_columns = [col.name for col in tmp_schema]
    insert_values = [f"S.{col}" for col in insert_columns]

    merge_query = f"""
    MERGE `{main_table}` T
    USING `{tmp_table}` S
    ON T.{primary_key} = S.{primary_key}
    WHEN MATCHED THEN
      UPDATE SET
        {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({', '.join(insert_columns)})
      VALUES({', '.join(insert_values)})
    """
    client.query(merge_query).result()
    logging.info(f"Merged tmp table {tmp_table} into main table {main_table}")

    tmp_table_ref = client.dataset(tmp_table)
    tmp_table_ref.expires = int(time.time() + ttl_hours * 3600) * 1000
    client.update_table(tmp_table_ref, ["expires"])
