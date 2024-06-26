from bucket import BucketFolder
from google.cloud import bigquery, storage
from tables_config import ref_tables
from utils import (
    BIGQUERY_IMPORT_BUCKET_FOLDER,
    DATA_BUCKET,
    PROJECT_NAME,
    RAW_DATASET,
    GCS_to_bigquery,
)

storage_client = storage.Client()
bigquery_client = bigquery.Client()


def run():
    for table, config in ref_tables.items():
        folder_name = BIGQUERY_IMPORT_BUCKET_FOLDER + "/" + table
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
            destination_dataset=RAW_DATASET,
            destination_table=table,
            schema=config.get("schema"),
        )

    return "Success"


run()
