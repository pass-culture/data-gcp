from google.cloud import storage
import os
import logging

ENV_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME", "")
DATA_GCS_BUCKET_NAME = os.environ.get(
    "DATA_GCS_BUCKET_NAME", f"data-bucket-{ENV_SHORT_NAME}"
)
GCP_PROJECT_ID = os.environ.get("PROJECT_NAME")
API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"

def get_update_since_param(dms_target):

    bucket_name = DATA_GCS_BUCKET_NAME
    prefix = "dms_export"
    storage_client = storage.Client()

    print(bucket_name)
    logging.info(f"Bucket : {bucket_name}")

    blobs = [
        (
            blob.name,
            blob.name.replace(f"{prefix}/unsorted_dms_{dms_target}_", "").replace(
                ".json", ""
            ),
        )
        for blob in storage_client.list_blobs(
            bucket_name,
            prefix=prefix,
        )
        if blob.name.startswith(f"{prefix}/unsorted_dms_{dms_target}")
    ]

    updated_since = [
        blob[1] for blob in blobs if blob[1] == max([blob[1] for blob in blobs])
    ][0][0:10]

    return updated_since
