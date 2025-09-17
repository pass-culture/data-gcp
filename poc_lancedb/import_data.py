import os
from google.cloud import bigquery, storage
from constants import BUCKET_NAME
# --- CONFIGURATION ---
# bq_table = "passculture-data-ehp.sandbox_stg.extract_item_embedding_chatbot_0825"
bq_table = "passculture-data-prod.sandbox_prod.chatbot_test_dataset_enriched"
gcs_bucket = "data-bucket-ml-temp-dev"
gcs_prefix = "chatbot_edito"
gcs_uri = f"gs://{gcs_bucket}/{gcs_prefix}/chatbot_test_dataset_enriched.parquet"
local_file = "chatbot_test_dataset_enriched.parquet"

# --- EXPORT BIGQUERY TABLE TO GCS ---
bq_client = bigquery.Client()
extract_job = bq_client.extract_table(
    bq_table,
    gcs_uri,py
    job_config=bigquery.job.ExtractJobConfig(destination_format="PARQUET"),
)
extract_job.result()
print(f"Exported BigQuery table to {gcs_uri}")

# --- DOWNLOAD FROM GCS TO LOCAL ---
storage_client = storage.Client()
bucket = storage_client.bucket(gcs_bucket)
blob = bucket.blob(f"{gcs_prefix}/chatbot_test_dataset_enriched.parquet")
blob.download_to_filename(local_file)
print(f"Downloaded {gcs_uri} to {local_file}")

def download_gcs_database(gcs_database_path: str):
        """
        Downloads a vector database from GCS to local machine
        :param gcs_database_path: The GCS path of the database to download
        """
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=gcs_database_path)

        for blob in blobs:
            if blob.name.endswith("/"):
                continue

            relative_path = os.path.relpath(blob.name, gcs_database_path)
            local_path = os.path.join(
                str(VECTOR_DB_PATH) + "/" + gcs_database_path.split("/")[-2],
                relative_path,
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            blob.download_to_filename(local_path)
            logger.info(f"Downloaded gs://{BUCKET_NAME}/{blob.name} → {local_path}")