from google.cloud import bigquery, storage

from bucket import BucketFolder
from tables_config import REF_TABLES
from utils import (
    BIGQUERY_IMPORT_BUCKET_FOLDER,
    DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME,
    PROJECT_NAME,
    SEED_DATASET,
    GCS_to_bigquery,
)

storage_client = storage.Client()
bigquery_client = bigquery.Client()


def run():
    for table, config in REF_TABLES.items():
        folder_name = f"{BIGQUERY_IMPORT_BUCKET_FOLDER}/{table}"
        print("Folder Name : ", folder_name)
        bucket_folder = BucketFolder(DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME, folder_name)
        file_name = bucket_folder.get_last_file_name(storage_client)
        print("File Name : ", file_name)

        GCS_to_bigquery(
            gcp_project=PROJECT_NAME,
            bigquery_client=bigquery_client,
            bucket_name=DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME,
            folder_name=folder_name,
            file_name=file_name,
            file_type=config.get("file_type"),
            destination_dataset=SEED_DATASET,
            destination_table=table,
            schema=config.get("schema"),
        )

    return "Success"


run()
