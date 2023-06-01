QPI_ANSWERS_SCHEMA = [
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "submitted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {
        "name": "answers",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "question_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "answer_ids", "type": "STRING", "mode": "REPEATED"},
        ],
    },
]

SQL_RAW_PATH = f"dependencies/qpi/sql/raw"
SQL_CLEAN_PATH = f"dependencies/qpi/sql/clean"
SQL_ANALYTICS_PATH = f"dependencies/qpi/sql/analytics"

RAW_TABLES = {
    "qpi_answers_v4": {
        "sql": f"{SQL_RAW_PATH}/qpi_answers_v4.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "qpi_answers_v4",
        "trigger_rule": "none_failed_or_skipped",
    }
}


CLEAN_TABLES = {
    "qpi_answers_v4": {
        "sql": f"{SQL_CLEAN_PATH}/qpi_answers_v4.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qpi_answers_v4",
        "trigger_rule": "none_failed_or_skipped",
    },
    "temp_qpi_answers_v4": {
        "sql": f"{SQL_CLEAN_PATH}/temp_qpi_answers_v4.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "temp_qpi_answers_v4",
        "trigger_rule": "none_failed_or_skipped",
    },
}

ANALYTICS_TABLES = {
    "enriched_qpi_answers_v4": {
        "sql": f"{SQL_ANALYTICS_PATH}/enriched_qpi_answers_v4.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "enriched_qpi_answers_v4",
        "trigger_rule": "none_failed_or_skipped",
    },
    "enriched_qpi_answers_projection": {
        "sql": f"{SQL_ANALYTICS_PATH}/enriched_qpi_answers_projection.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "enriched_qpi_answers_projection",
        "trigger_rule": "none_failed_or_skipped",
    },
    "enriched_aggregated_qpi_answers": {
        "sql": f"{SQL_ANALYTICS_PATH}/enriched_aggregated_qpi_answers.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "enriched_aggregated_qpi_answers",
        "trigger_rule": "none_failed_or_skipped",
        "depends": ["enriched_qpi_answers_v4"],
    },
}
