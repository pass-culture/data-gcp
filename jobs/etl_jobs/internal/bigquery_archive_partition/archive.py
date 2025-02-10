from utils import ENV_SHORT_NAME, GCP_PROJECT_ID, EXPORT_PATH
from google.cloud import bigquery


def store_partitions_in_temp(table, config):
    client = bigquery.Client()

    partition_column = config["partition_column"]
    dataset_id = config["dataset_id"]
    look_back_months = config["look_back_months"][ENV_SHORT_NAME]

    query_temp = f"""CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.tmp_{ENV_SHORT_NAME}.{table}_old_partitions`
        as
        SELECT *, {partition_column} as partition_date
        FROM `{GCP_PROJECT_ID}.{dataset_id}.{table}`
        WHERE {partition_column} < DATE_SUB(CURRENT_DATE(), INTERVAL {look_back_months} MONTH);
    """

    query_temp_job = client.query(query_temp)
    query_temp_job.result()


def export_partitions(table, config):
    client = bigquery.Client()

    query_partitions_job = client.query(f"""
        SELECT DISTINCT partition_date
        FROM `{GCP_PROJECT_ID}.tmp_{ENV_SHORT_NAME}.{table}_old_partitions`
    """)

    partitions = [row["partition_date"] for row in query_partitions_job.result()]

    if not partitions:
        print("No partitions to export.")

    look_back_months = config["look_back_months"][ENV_SHORT_NAME]
    dataset_id = config["dataset_id"]
    partition_column = config["partition_column"]

    for partition_date in partitions:
        partition_str = partition_date.strftime("%Y%m%d")  # Convert to YYYYMMDD format
        gcs_uri = f"{EXPORT_PATH}/{table}/{partition_str}_{table}_*.parquet"
        query_export = f"""
            EXPORT DATA
            OPTIONS (
                uri='{gcs_uri}',
                format='PARQUET',
                overwrite=true
            )
            AS
            SELECT *
            FROM `passculture-data-ehp.raw_dev.past_offer_context`
            WHERE DATE(import_date) = '{partition_date}';
        """
        query_export_job = client.query(query_export)
        query_export_job.result()


def delete_partitions(table, config):
    client = bigquery.Client()
    dataset_id = config["dataset_id"]
    partition_column = config["partition_column"]
    look_back_months = config["look_back_months"][ENV_SHORT_NAME]

    query_delete = f"""DELETE FROM `{GCP_PROJECT_ID}.{dataset_id}.{table}`
        WHERE {partition_column} < DATE_SUB(CURRENT_DATE(), INTERVAL {look_back_months} MONTH);
    """

    query_delete_job = client.query(query_delete)
    query_delete_job.result()
