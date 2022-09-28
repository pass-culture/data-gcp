from common.config import (
    GCP_PROJECT,
    ENV_SHORT_NAME,
)
from common.access_gcp_secrets import access_secret_data

SQL_PATH = f"dependencies/qualtrics/sql/clean"

export_tables = {
    "qualtrics_ir_jeunes": {
        "sql": f"{SQL_PATH}/qualtrics_ir_jeunes.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qualtrics_ir_jeunes${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "cluster_fields": ["calculation_month"],
        "params": {"volume": 8000 if ENV_SHORT_NAME == "prod" else 10},
        "qualtrics_automation_id": access_secret_data(
            GCP_PROJECT, f"qualtrics_ir_jeunes_automation_id_{ENV_SHORT_NAME}"
        ),
        "include_email": True
    },
    "qualtrics_ac": {
        "sql": f"{SQL_PATH}/qualtrics_ac.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qualtrics_ac${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "cluster_fields": ["calculation_month"],
        "params": {"volume": 4000 if ENV_SHORT_NAME == "prod" else 10},
        "qualtrics_automation_id": access_secret_data(
            GCP_PROJECT, f"qualtrics_ir_ac_automation_id_{ENV_SHORT_NAME}"
        ),
        "include_email": False
    },
}
