import os

import pandas as pd
from google.cloud import bigquery

GCP_PROJECT = os.environ["GCP_PROJECT_ID"]


def chunks(input_list: list, size: int):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(input_list), size):
        yield input_list[i : i + size]


def save(df: pd.DataFrame, dataset_id: str, table_name: str):
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="APPEND",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
    )
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def query(dataset_id: str, table_name: str, limit: int = 1000):
    query_string = f"SELECT user_id, full_address FROM {dataset_id}.{table_name} ORDER BY user_creation_date DESC LIMIT {limit} "
    return bigquery.Client().query(query_string).to_dataframe()
