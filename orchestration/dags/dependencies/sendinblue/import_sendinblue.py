SQL_PATH = "dependencies/sendinblue/sql"

raw_tables = {
    "sendinblue_transactional_raw": {
        "sql": f"{SQL_PATH}/raw/sendinblue_transactional.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "sendinblue_transactional${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "execution_date"},
    },
}

clean_tables = {
    "sendinblue_transactional_clean": {
        "sql": f"{SQL_PATH}/clean/sendinblue_transactional.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "sendinblue_transactional${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "update_date"},
    },
}


analytics_tables = {
    "sendinblue_newsletters_performance": {
        "sql": f"{SQL_PATH}/analytics/sendinblue_newsletters_performance.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "sendinblue_newsletters_performance",
    },
    "sendinblue_transactional_performance": {
        "sql": f"{SQL_PATH}/analytics/sendinblue_transactional_performance.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "sendinblue_transactional_performance",
    },
}
