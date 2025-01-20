import typer
from transfer import CloudStorageTransfer
from key_management import generate_unique_key, log_key
from export_logs import get_partner_config

run = typer.Typer()


@run.command()
def transfer_files(
    gcs_bucket_name: str,
    s3_bucket_name: str,
    s3_endpoint_url: str,
    s3_access_key: str,
    s3_secret_key: str,
    gcs_folder_path: str,
    gcs_encrypted_folder_path: str,
    s3_prefix_export: str,
    encryption_key_name: str,
    encryption_key: str,
    s3_region_name: str = "us-east-1",
):
    """
    Transfers and encrypts Parquet files from Google Cloud Storage (GCS) to an S3-compatible service.

    Args:
        gcs_bucket_name (str): Name of the GCS bucket containing source files.
        s3_bucket_name (str): Name of the destination S3 bucket.
        s3_endpoint_url (str): Endpoint URL for the S3-compatible service.
        s3_access_key (str): Access key for the S3-compatible service.
        s3_secret_key (str): Secret key for the S3-compatible service.
        gcs_folder_path (str): Path to the source folder in GCS containing the Parquet files.
        gcs_encrypted_folder_path (str): Path in GCS where encrypted files will be stored.
        s3_prefix_export (str): Prefix for the destination path in the S3 bucket.
        encryption_key_name (str): Identifier for the encryption key to use.
        encryption_key (str): 256-bit encryption key as a string.
        s3_region_name (str, optional): Region name for the S3-compatible service. Defaults to 'us-east-1'.
    """
    partner_config = get_partner_config()

    transfer = CloudStorageTransfer(
        gcs_bucket_name=gcs_bucket_name,
        s3_bucket_name=s3_bucket_name,
        s3_endpoint_url=s3_endpoint_url,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_region_name=s3_region_name,
    )

    encryption_key_name, encryption_key = generate_unique_key()
    log_key(encryption_key_name, encryption_key)
    transfer.add_encryption_key(encryption_key_name, encryption_key)
    transfer.process_files(
        gcs_folder_path=gcs_folder_path,
        gcs_encrypted_folder_path=gcs_encrypted_folder_path,
        s3_prefix_export=s3_prefix_export,
        encryption_key_name=encryption_key_name,
    )


if __name__ == "__main__":
    run()
