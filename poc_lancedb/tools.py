import pyarrow as pa
import pyarrow.parquet as pq
import os
from google.cloud import storage
from loguru import logger
from constants import BUCKET_NAME,GCS_VECTOR_DB_PATH
# 3. Use real Parquet file if it exists, otherwise generate dummy data
def create_dummy_table(parquet_file_path, data_size=500, vector_dim=384):
    """
    Ensures that the specified Parquet file exists. If not, generates dummy data and writes it to the file.
    """
    logger.warning(f"Parquet file '{parquet_file_path}' not found. Generating dummy data.")
    ids = [f"item_{i:03d}" for i in range(data_size)]
    vectors = np.random.rand(data_size, vector_dim).astype(np.float32)
    texts = [
        f"This is a dummy text description for item {i}. It covers topics like data science, AI, and machine learning. This is more text to make it longer and more diverse."
        for i in range(data_size)
    ]

    # Use PyArrow to create a FixedSizeListArray for the vector column
    
    ids_array = pa.array(ids, pa.string())
    texts_array = pa.array(texts, pa.string())
    vector_flat = vectors.flatten().tolist()
    vector_value_array = pa.array(vector_flat, pa.float32())
    vector_list_array = pa.FixedSizeListArray.from_arrays(vector_value_array, vector_dim)

    table = pa.table({
        'item_id': ids_array,
        'embedding': vector_list_array,
        'offer_description': texts_array
    })
    # Write to Parquet
    pq.write_table(table, parquet_file_path)
    logger.info(f"Generated dummy parquet file for direct import: {parquet_file_path} with {data_size} records.")


# --- DOWNLOAD FROM GCS TO LOCAL ---
bq_table = "passculture-data-prod.sandbox_prod.chatbot_test_dataset_enriched"
gcs_bucket = "data-bucket-ml-temp-dev"
gcs_prefix = "chatbot_edito"
gcs_uri = f"gs://{gcs_bucket}/{gcs_prefix}/chatbot_test_dataset_enriched.parquet"
local_file = "chatbot_test_dataset_enriched.parquet"
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
                ".",
                relative_path,
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            blob.download_to_filename(local_path)
            logger.info(f"Downloaded gs://{BUCKET_NAME}/{blob.name} → {local_path}")