import duckdb
from google.cloud import storage
import boto3
from botocore.client import Config


class CloudStorageTransfer:
    """
    Manages the transfer of Parquet files between Google Cloud Storage (GCS) and an S3-compatible service,
    including encryption using DuckDB.

    Attributes:
        gcs_client (google.cloud.storage.Client): Client to interact with GCS.
        s3_client (boto3.client): Client to interact with the S3-compatible service.
        s3_bucket_name (str): Name of the destination S3 bucket.
        duckdb_conn (duckdb.DuckDBPyConnection): DuckDB connection for encrypting Parquet files.
    """

    def __init__(
        self,
        gcs_bucket_name,
        s3_bucket_name,
        s3_endpoint_url,
        s3_access_key,
        s3_secret_key,
        s3_region_name="us-east-1",
    ):
        """
        Initializes the CloudStorageTransfer with GCS and S3 clients, and sets up a DuckDB connection.

        Args:
            gcs_bucket_name (str): Name of the GCS bucket containing source files.
            s3_bucket_name (str): Name of the destination S3 bucket.
            s3_endpoint_url (str): Endpoint URL for the S3-compatible service.
            s3_access_key (str): Access key for the S3-compatible service.
            s3_secret_key (str): Secret key for the S3-compatible service.
            s3_region_name (str, optional): Region name for the S3-compatible service. Defaults to 'us-east-1'.
        """
        self.gcs_bucket_name = gcs_bucket_name
        self.s3_bucket_name = s3_bucket_name
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            endpoint_url=s3_endpoint_url,
            region_name=s3_region_name,
            config=Config(signature_version="s3v4"),
        )
        self.duckdb_conn = duckdb.connect()
        self.duckdb_conn.execute("INSTALL httpfs; LOAD httpfs;")

    def add_encryption_key(self, key_name, encryption_key):
        """
        Adds an encryption key to the DuckDB session.

        Args:
            key_name (str): Identifier for the encryption key.
            encryption_key (str): 256-bit encryption key as a string.
        """
        self.duckdb_conn.execute(
            f"PRAGMA add_parquet_key('{key_name}', '{encryption_key}');"
        )

    def process_files_directly(
        self, gcs_folder_path, s3_prefix_export, encryption_key_name
    ):
        """
        Processes files directly between GCS and S3 using DuckDB without local filesystem.

        Args:
            gcs_folder_path (str): Path to the folder in GCS containing the Parquet files.
            s3_prefix_export (str): Prefix for the destination path in the S3 bucket.
            encryption_key_name (str): Identifier for the encryption key to use.
        """
        # List files in the GCS bucket
        gcs_url = f"gs://{self.gcs_bucket_name}/{gcs_folder_path}/*.parquet"
        files_query = (
            f"SELECT DISTINCT filename FROM read_parquet('{gcs_url}', filename=true);"
        )
        files = self.duckdb_conn.execute(files_query).fetchall()

        if not files:
            print(f"No Parquet files found in {gcs_folder_path}")
            return

        for file in files:
            file_path = file[0]
            encrypted_file_path = f"s3://{self.s3_bucket_name}/{s3_prefix_export}/{file_path.split('/')[-1]}"

            # Encrypt and transfer the file directly
            self.duckdb_conn.execute(
                f"""
                COPY (
                    SELECT * FROM read_parquet('{file_path}')
                ) TO '{encrypted_file_path}' (
                    FORMAT 'parquet', ENCRYPTION_CONFIG {{footer_key: '{encryption_key_name}'}}
                );
                """
            )

            print(f"Encrypted and transferred: {file_path} to {encrypted_file_path}")

        print("File processing and transfer completed.")
