import os

GCP_PROJECT = os.environ.get("PROJECT_NAME")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "")
BIGQUERY_CLEAN_DATASET = os.environ.get(
    "BIGQUERY_CLEAN_DATASET", f"clean_{ENV_SHORT_NAME}"
)
BUCKET_NAME = os.environ["BUCKET_NAME"]
API_KEY = "4e2a0208-cfa8-4294-bceb-e24a3277d842"
ENDPOINT = "https://omogen-api-pr.phm.education.gouv.fr/adage-api-test/v1"
