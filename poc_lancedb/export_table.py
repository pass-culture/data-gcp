import os
from google.cloud import storage
from loguru import logger
from constants import DB_PATH, BUCKET_NAME,TABLE_NAME,GCS_VECTOR_DB_PATH
def upload_database_to_gcs(path: str,table_name: str):
        """
        Uploads the contents of a local directory to a Google Cloud Storage bucket path.

        :param model_name: model_name whose vector database is to upload
        """
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        database_local_path = path
        destination_path = f"{GCS_VECTOR_DB_PATH}/{table_name.replace('/', '_').replace('-', '_')}.lance"

        for root, _, files in os.walk(database_local_path):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(local_path, database_local_path)
                gcs_path = os.path.join(destination_path, relative_path).replace(
                    "\\", "/"
                )  # GCS uses '/' for paths

                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_path)
                logger.info(
                    f"Uploaded {local_path} to gs://{BUCKET_NAME}/{gcs_path}"
                )
upload_database_to_gcs(DB_PATH,TABLE_NAME)