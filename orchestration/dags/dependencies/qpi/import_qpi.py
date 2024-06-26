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

SQL_RAW_PATH = "dependencies/qpi/sql/raw"
SQL_CLEAN_PATH = "dependencies/qpi/sql/clean"
SQL_ANALYTICS_PATH = "dependencies/qpi/sql/analytics"

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
    "qpi_answers_historical_clean": {
        "sql": f"{SQL_CLEAN_PATH}/qpi_answers_historical_clean.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qpi_answers_historical_clean",
        "trigger_rule": "none_failed_or_skipped",
        "load_table": False,
    },
    "qpi_answers_v4_clean": {
        "sql": f"{SQL_CLEAN_PATH}/qpi_answers_v4_clean.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qpi_answers_v4_clean",
        "time_partitioning": {"field": "submitted_at"},
        "clustering_fields": {"fields": ["submitted_at"]},
        "trigger_rule": "none_failed_or_skipped",
    },
    "qpi_answers_projection": {
        "sql": f"{SQL_CLEAN_PATH}/qpi_answers_projection.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "qpi_answers_projection",
        "time_partitioning": {"field": "submitted_at"},
        "clustering_fields": {"fields": ["submitted_at"]},
        "trigger_rule": "none_failed_or_skipped",
    },
}

ANALYTICS_TABLES = {
    "enriched_qpi_answers_projection": {
        "sql": f"{SQL_ANALYTICS_PATH}/enriched_qpi_answers_projection.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "enriched_qpi_answers_projection",
        "trigger_rule": "none_failed_or_skipped",
        "time_partitioning": {"field": "submitted_at"},
        "clustering_fields": {"fields": ["submitted_at"]},
    },
    "enriched_qpi_answers": {
        "sql": f"{SQL_ANALYTICS_PATH}/enriched_qpi_answers.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "enriched_qpi_answers",
        "trigger_rule": "none_failed_or_skipped",
        "time_partitioning": {"field": "submitted_at"},
        "clustering_fields": {"fields": ["submitted_at"]},
    },
}
