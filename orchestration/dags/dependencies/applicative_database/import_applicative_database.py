from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
)
from common.utils import get_tables_config_dict

RAW_SQL_PATH = "dependencies/applicative_database/sql/raw"
CLEAN_HISTORY_SQL_PATH = "dependencies/applicative_database/sql/clean/history"

PARALLEL_TABLES = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH + "/parallel",
    BQ_DATASET=BIGQUERY_RAW_DATASET,
)

SEQUENTIAL_TABLES = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH + "/sequential",
    BQ_DATASET=BIGQUERY_RAW_DATASET,
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
