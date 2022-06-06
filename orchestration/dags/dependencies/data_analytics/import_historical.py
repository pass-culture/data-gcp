SQL_PATH = "dependencies/data_analytics/sql/clean/applicative_database/"


historical_data_applicative_tables = {
    "booking_history": {
        "sql": f"{SQL_PATH}/booking_history.sql",
        "destination_dataset_table": "{{ bigquery_clean_dataset }}.booking_history${{ yyyymmdd() }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_booking_history": {
        "sql": f"{SQL_PATH}/collective_booking_history.sql",
        "destination_dataset_table": "{{ bigquery_clean_dataset }}.collective_booking_history${{ yyyymmdd() }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_offer_history": {
        "sql": f"{SQL_PATH}/collective_offer_history.sql",
        "destination_dataset_table": "{{ bigquery_clean_dataset }}.collective_offer_history${{ yyyymmdd() }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_stock_history": {
        "sql": f"{SQL_PATH}/collective_stock_history.sql",
        "destination_dataset_table": "{{ bigquery_clean_dataset }}.collective_stock_history${{ yyyymmdd() }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "offer_history": {
        "sql": f"{SQL_PATH}/offer_history.sql",
        "destination_dataset_table": "{{ bigquery_clean_dataset }}.offer_history${{ yyyymmdd() }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "stock_history": {
        "sql": f"{SQL_PATH}/stock_history.sql",
        "destination_dataset_table": "{{ bigquery_clean_dataset }}.stock_history${{ yyyymmdd() }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
}
