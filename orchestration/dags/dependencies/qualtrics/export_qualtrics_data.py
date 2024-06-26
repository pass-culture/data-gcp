from common.access_gcp_secrets import access_secret_data
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)

SQL_PATH = "dependencies/qualtrics/sql"

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
    "enriched_qualtrics_pro": {
        "sql": f"{SQL_PATH}/analytics/enriched_qualtrics_pro.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "enriched_qualtrics_pro",
        "dag_depends": ["import_analytics_v7"],
        "depends": ["qualtrics_answers_ir_pro"],
    },
}
