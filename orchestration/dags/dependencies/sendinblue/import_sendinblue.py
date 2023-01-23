SQL_PATH = f"dependencies/sendinblue/sql"


analytics_tables = {
    "sendinblue_newsletters_performance": {
        "sql": f"{SQL_PATH}/analytics/sendinblue_newsletters_performance.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "sendinblue_newsletters_performance",
    },
}
