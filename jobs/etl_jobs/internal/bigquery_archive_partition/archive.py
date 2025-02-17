from utils import ENV_SHORT_NAME, GCP_PROJECT_ID, EXPORT_PATH
from google.cloud import bigquery


class Archive:
    def __init__(self, table, config):
        self.table = table
        self.config = config
        self.partition_column = config["partition_column"]
        self.dataset_id = config["dataset_id"]
        self.client = bigquery.Client()
        self.folder = config["folder"]

        if ENV_SHORT_NAME == "prod":
            self.look_back_months = config["look_back_months"]
        else:
            self.look_back_months = 3

    def store_partitions_in_temp(self):
        query_temp = f"""CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.tmp_{ENV_SHORT_NAME}.{self.table}_old_partitions`
            as
            SELECT *, {self.partition_column} as partition_date
            FROM `{GCP_PROJECT_ID}.{self.dataset_id}.{self.table}`
            WHERE {self.partition_column} < DATE_SUB(CURRENT_DATE(), INTERVAL {self.look_back_months} MONTH);
        """

        query_temp_job = self.client.query(query_temp)
        query_temp_job.result()

    def export_partitions(self):
        query_partitions_job = self.client.query(f"""
            SELECT DISTINCT partition_date
            FROM `{GCP_PROJECT_ID}.tmp_{ENV_SHORT_NAME}.{self.table}_old_partitions`
        """)

        partitions = [row["partition_date"] for row in query_partitions_job.result()]

        if not partitions:
            print("No partitions to export.")

        for partition_date in partitions:
            partition_str = partition_date.strftime(
                "%Y%m%d"
            )  # Convert to YYYYMMDD format
            gcs_uri = f"{EXPORT_PATH}/{self.folder}/{self.table}/{partition_str}_{self.table}_*.parquet"
            query_export = f"""
                EXPORT DATA
                OPTIONS (
                    uri='{gcs_uri}',
                    format='PARQUET',
                    overwrite=true
                )
                AS
                SELECT *
                FROM `{GCP_PROJECT_ID}.{self.dataset_id}.{self.table}`
                WHERE DATE({self.partition_column}) = '{partition_date}';
            """
            query_export_job = self.client.query(query_export)
            query_export_job.result()

    def delete_partitions(self):
        query_delete = f"""DELETE FROM `{GCP_PROJECT_ID}.{self.dataset_id}.{self.table}`
            WHERE {self.partition_column} < DATE_SUB(CURRENT_DATE(), INTERVAL {self.look_back_months} MONTH);
        """

        query_delete_job = self.client.query(query_delete)
        query_delete_job.result()
