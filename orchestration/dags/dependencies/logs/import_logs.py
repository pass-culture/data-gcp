SQL_PATH = f"dependencies/logs/sql/analytics"


import_tables = {
    "adage_logs": {
        "sql": f"{SQL_PATH}/adage_logs.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "adage_logs${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "collective_logs": {
        "sql": f"{SQL_PATH}/collective_logs.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "collective_logs${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "app_native_logs": {
        "sql": f"{SQL_PATH}/app_native_logs.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "app_native_logs${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "app_pro_logs": {
        "sql": f"{SQL_PATH}/app_pro_logs.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "app_pro_logs${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
}
