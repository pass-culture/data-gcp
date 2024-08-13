import logging
import math

from google.cloud import storage


def compose_gcs_files(bucket_name, source_prefix, destination_blob_name):
    """
    Concatenate files, identified with their prefix (for example "bigquery_exports/offer-"),
    into destination blob.
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    sources = list(storage_client.list_blobs(bucket, prefix=source_prefix))
    if len(sources) > 32:
        loop = math.ceil(len(sources) / 32)
        file_name_splitted = destination_blob_name.split(".")
        for i in range(loop):
            destination = bucket.blob(
                f"{file_name_splitted[0]}_temp_{i}.{file_name_splitted[1]}"
            )
            destination.content_type = "text/plain"
            destination.compose(sources[i * 32 : (i + 1) * 32])
        temp_sources = list(
            storage_client.list_blobs(bucket, prefix=f"{file_name_splitted[0]}_temp_")
        )
        destination = bucket.blob(destination_blob_name)
        destination.content_type = "text/plain"
        destination.compose(temp_sources)
        for i in range(loop):
            blob = bucket.blob(
                f"{file_name_splitted[0]}_temp_{i}.{file_name_splitted[1]}"
            )
            blob.delete()

    else:
        destination = bucket.blob(destination_blob_name)
        destination.content_type = "text/plain"
        destination.compose(sources)

    with storage_client.batch():
        for blob in sources:
            blob.delete()

    logging.info(
        "Composed new object %s in the bucket %s", destination_blob_name, bucket.name
    )
