SQL_PATH = f"dependencies/logs/sql/analytics"


import_tables = {
    "adage_logs": {
        "sql": f"{SQL_PATH}/adage_logs.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.adage_logs${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_logs": {
        "sql": f"{SQL_PATH}/collective_logs.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.collective_logs${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
}
