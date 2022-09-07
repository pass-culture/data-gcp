import os
from google.cloud import bigquery
from datetime import datetime

GCP_PROJECT = os.environ.get("PROJECT_NAME")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "")
BIGQUERY_ANALYTICS_DATASET = os.environ.get(
    "BIGQUERY_ANALYTICS_DATASET", f"analytics_{ENV_SHORT_NAME}"
)
BUCKET_NAME = os.environ["BUCKET_NAME"]


def save_to_raw_bq(df, table_name):
    _now = datetime.today()
    yyyymmdd = _now.strftime("%Y%m%d")
    df["execution_date"] = _now.strftime("%Y-%m-%d")
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
        ),
    )
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
