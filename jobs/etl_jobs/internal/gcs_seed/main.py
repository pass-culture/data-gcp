from google.cloud import storage
from google.cloud import bigquery
from utils import (
    PROJECT_NAME,
    SEED_DATASET,
    DATA_BUCKET,
    BIGQUERY_IMPORT_BUCKET_FOLDER,
    GCS_to_bigquery,
)

from tables_config import REF_TABLES
from bucket import BucketFolder

storage_client = storage.Client()
bigquery_client = bigquery.Client()


def run():
    for table, config in REF_TABLES.items():
        folder_name = f"{BIGQUERY_IMPORT_BUCKET_FOLDER}/{table}"
        print("Folder Name : ", folder_name)
        bucket_folder = BucketFolder(DATA_BUCKET, folder_name)
        file_name = bucket_folder.get_last_file_name(storage_client)
        print("File Name : ", file_name)

        GCS_to_bigquery(
            gcp_project=PROJECT_NAME,
            bigquery_client=bigquery_client,
            bucket_name=DATA_BUCKET,
            folder_name=folder_name,
            file_name=file_name,
            file_type=config.get("file_type"),
            destination_dataset=SEED_DATASET,
            destination_table=table,
            schema=config.get("schema"),
        )

    return "Success"


run()
