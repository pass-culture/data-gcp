SQL_PATH = "dependencies/brevo/sql"

raw_tables = {
    "brevo_transactional_raw": {
        "sql": f"{SQL_PATH}/raw/brevo_transactional.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "brevo_transactional${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "execution_date"},
    },
}

clean_tables = {
    "brevo_transactional_clean": {
        "sql": f"{SQL_PATH}/clean/brevo_transactional.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "brevo_transactional${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "update_date"},
    },
}


analytics_tables = {
    "brevo_newsletters_performance": {
        "sql": f"{SQL_PATH}/analytics/brevo_newsletters_performance.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "brevo_newsletters_performance",
    },
    "brevo_transactional_performance": {
        "sql": f"{SQL_PATH}/analytics/brevo_transactional_performance.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "brevo_transactional_performance",
    },
}
