from common.config import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
)
from common.access_gcp_secrets import access_secret_data

SQL_PATH = f"dependencies/qualtrics/sql"

clean_tables = {
    "qualtrics_ir_jeunes": {
        "sql": f"{SQL_PATH}/clean/qualtrics_ir_jeunes.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qualtrics_ir_jeunes${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "params": {"volume": 8000 if ENV_SHORT_NAME == "prod" else 10},
        "qualtrics_automation_id": access_secret_data(
            GCP_PROJECT_ID, f"qualtrics_ir_jeunes_automation_id_{ENV_SHORT_NAME}"
        ),
        "include_email": True,
    },
    "qualtrics_ac": {
        "sql": f"{SQL_PATH}/clean/qualtrics_ac.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qualtrics_ac${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "params": {"volume": 10000 if ENV_SHORT_NAME == "prod" else 10},
        "qualtrics_automation_id": access_secret_data(
            GCP_PROJECT_ID, f"qualtrics_ir_ac_automation_id_{ENV_SHORT_NAME}"
        ),
        "include_email": False,
    },
}

analytics_tables = {
    "qualtrics_answers_ir_jeunes": {
        "sql": f"{SQL_PATH}/analytics/qualtrics_answers_ir_jeunes.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "qualtrics_answers_ir_jeunes",
    },
    "qualtrics_answers_ir_pro": {
        "sql": f"{SQL_PATH}/analytics/qualtrics_answers_ir_pro.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "qualtrics_answers_ir_pro",
    },
}
