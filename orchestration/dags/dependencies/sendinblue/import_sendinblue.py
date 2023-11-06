SQL_PATH = f"dependencies/sendinblue/sql"

clean_tables = {
    "sendinblue_transactional": {
        "sql": f"{SQL_PATH}/analytics/sendinblue_transactional.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "sendinblue_transactional",
        "time_partitioning": {"field": "update_date"},
        "partition_prefix": "$",
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
