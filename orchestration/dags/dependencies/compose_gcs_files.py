import logging
from google.cloud import storage


def compose_gcs_files(bucket_name, source_prefix, destination_blob_name):
    """
    Concatenate files, identified with their prefix (for example "bigquery_exports/offer-"),
    into destination blob.
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination = bucket.blob(destination_blob_name)
    destination.content_type = "text/plain"
    sources = list(storage_client.list_blobs(bucket, prefix=source_prefix))
    destination.compose(sources)

    logging.info(
        "Composed new object %s in the bucket %s", destination_blob_name, bucket.name
    )
