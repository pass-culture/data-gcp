import pandas as pd
from google.cloud import bigquery

from src.utils.constants import FINANCE_DATASET, GCP_PROJECT_ID


def get_client() -> bigquery.Client:
    return bigquery.Client()


def load_table(table_name) -> pd.DataFrame:
    client = get_client()
    query = f"SELECT * FROM `{GCP_PROJECT_ID}.{FINANCE_DATASET}.{table_name}`"
    return client.query(query).to_dataframe()


def load_query(query: str) -> pd.DataFrame:
    client = get_client()
    return client.query(query).to_dataframe()


def save_df_as_bigquery_table(df: pd.DataFrame, table_name: str):
    client = get_client()
    table_id = f"{GCP_PROJECT_ID}.{FINANCE_DATASET}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
