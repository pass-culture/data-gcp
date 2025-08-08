from google.cloud import bigquery, storage
import os

# --- CONFIGURATION ---
bq_table = "passculture-data-ehp.sandbox_stg.extract_item_embedding_chatbot_0825"
gcs_bucket = "data-bucket-ml-temp-dev"
gcs_prefix = "chatbot_edito"
gcs_uri = f"gs://{gcs_bucket}/{gcs_prefix}/item_embedding_chatbot.parquet"
local_file = "item_embedding_chatbot.parquet"

# --- EXPORT BIGQUERY TABLE TO GCS ---
bq_client = bigquery.Client()
extract_job = bq_client.extract_table(
    bq_table,
    gcs_uri,
    job_config=bigquery.job.ExtractJobConfig(
        destination_format="PARQUET"
    ),
)
extract_job.result()
print(f"Exported BigQuery table to {gcs_uri}")

# --- DOWNLOAD FROM GCS TO LOCAL ---
storage_client = storage.Client()
bucket = storage_client.bucket(gcs_bucket)
blob = bucket.blob(f"{gcs_prefix}/item_embedding_chatbot.parquet")
blob.download_to_filename(local_file)
print(f"Downloaded {gcs_uri} to {local_file}")