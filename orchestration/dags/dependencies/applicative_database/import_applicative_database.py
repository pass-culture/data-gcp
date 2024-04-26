import os
from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
)

RAW_SQL_PATH = f"dependencies/applicative_database/sql/raw"
CLEAN_HISTORY_SQL_PATH = f"dependencies/applicative_database/sql/clean/history"


def get_tables_config_dict(PATH, BQ_DESTINATION_DATASET):
    tables_config = {}
    for file in os.listdir(PATH):
        extension = file.split(".")[-1]
        table_name = file.split(".")[0]
        if extension == "sql":
            tables_config[table_name] = {}
            tables_config[table_name]["sql"] = PATH + "/" + file
            tables_config[table_name]["destination_dataset"] = BQ_DESTINATION_DATASET
            tables_config[table_name][
                "destination_table"
            ] = f"applicative_database_{table_name}"
    return tables_config


RAW_TABLES = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH, BQ_DESTINATION_DATASET=BIGQUERY_RAW_DATASET
)


HISTORICAL_CLEAN_APPLICATIVE_TABLES = {
    "booking_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/booking_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_booking_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "collective_booking_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/collective_booking_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_booking_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "collective_offer_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/collective_offer_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_offer_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "collective_offer_template_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/collective_offer_template_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_offer_template_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "collective_stock_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/collective_stock_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_collective_stock_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "offer_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/offer_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_offer_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "offerer_tag_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/offerer_tag_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_offerer_tag_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "offerer_tag_mapping_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/offerer_tag_mapping_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_offerer_tag_mapping_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "offerer_tag_category_mapping_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/offerer_tag_category_mapping_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_offerer_tag_category_mapping_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "opening_hours": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/opening_hours.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_opening_hours${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "stock_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/stock_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_stock_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "venue_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/venue_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_venue_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
    "venue_criterion_history": {
        "sql": f"{CLEAN_HISTORY_SQL_PATH}/venue_criterion_history.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_venue_criterion_history${{ yyyymmdd(yesterday()) }}",
        "time_partitioning": {"field": "partition_date"},
        "clustering_fields": {"fields": ["partition_date"]},
    },
}
