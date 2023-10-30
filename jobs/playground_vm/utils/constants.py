import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_ANALYTICS_DATASET =  "analytics_prod" if ENV_SHORT_NAME == "prod" else "analytics_stg" if ENV_SHORT_NAME=="stg" else "analytics_dev"

print(f"{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.enriched_item_metadata")