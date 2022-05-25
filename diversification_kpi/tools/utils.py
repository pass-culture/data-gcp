import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "project-test-ci")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "project-test-ci")
GCP_REGION = "europe-west1"
GCE_ZONE = "europe-west1-b"
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")


APPLICATIVE_EXTERNAL_CONNECTION_ID = os.environ.get(
    "APPLICATIVE_EXTERNAL_CONNECTION_ID", ""
)

BIGQUERY_ANALYTICS_DATASET = os.environ.get(
    "BIGQUERY_ANALYTICS_DATASET", f"analytics_{ENV_SHORT_NAME}"
)

DATA_GCS_BUCKET_NAME = os.environ.get(
    "DATA_GCS_BUCKET_NAME", f"data-bucket-{ENV_SHORT_NAME}"
)

TABLE_NAME = os.environ.get("TABLE_NAME", f"diversification_bookings")
