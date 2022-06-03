SQL_PATH = "dependencies/data_analytics/sql/clean/applicative_database/"


historical_data_applicative_tables = {
    "collective_offer_history": {
        "sql": f"{SQL_PATH}/collective_offer_history.sql",
        "destination_dataset_table": "collective_offer_history",
        "time_partitioning": {"time_partitioning_type": "DAY"},
        "cluster_fields": ["partition_date"],
    },
    "offer_history": {
        "sql": f"{SQL_PATH}/offer_history.sql",
        "destination_dataset_table": "offer_history",
        "time_partitioning": {"time_partitioning_type": "DAY"},
        "cluster_fields": ["partition_date"],
    },
}
