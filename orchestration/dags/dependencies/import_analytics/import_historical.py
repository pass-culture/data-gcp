CLEAN_APPLICATIVE_DATABASE_SQL_PATH = (
    f"dependencies/import_analytics/sql/clean/applicative_database"
)
ANALYTICS_SQL_PATH = f"dependencies/import_analytics/sql/analytics"


historical_clean_applicative_database = {
    "booking_history": {
        "sql": f"{CLEAN_APPLICATIVE_DATABASE_SQL_PATH}/booking_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_booking_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_booking_history": {
        "sql": f"{CLEAN_APPLICATIVE_DATABASE_SQL_PATH}/collective_booking_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_booking_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_offer_history": {
        "sql": f"{CLEAN_APPLICATIVE_DATABASE_SQL_PATH}/collective_offer_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_offer_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_offer_template_history": {
        "sql": f"{CLEAN_APPLICATIVE_DATABASE_SQL_PATH}/collective_offer_template_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_offer_template_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "collective_stock_history": {
        "sql": f"{CLEAN_APPLICATIVE_DATABASE_SQL_PATH}/collective_stock_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_stock_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "offer_history": {
        "sql": f"{CLEAN_APPLICATIVE_DATABASE_SQL_PATH}/offer_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_offer_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "stock_history": {
        "sql": f"{CLEAN_APPLICATIVE_DATABASE_SQL_PATH}/stock_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_stock_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
}

historical_analytics = {
    "bookable_collective_offer_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/bookable_collective_offer_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "bookable_collective_offer_history${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
    "bookable_offer_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/bookable_offer_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "bookable_offer_history${{ yyyymmdd(ds) }}",
        "time_partitioning": {"field": "partition_date"},
        "cluster_fields": ["partition_date"],
    },
}
