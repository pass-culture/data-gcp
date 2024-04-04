from common.config import (
    ENV_SHORT_NAME,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    DAG_FOLDER,
)

from dependencies.import_analytics.import_raw import (
    get_tables_config_dict,
    RAW_SQL_PATH,
)

CLEAN_SQL_PATH = f"dependencies/import_analytics/sql/clean"

clean_tables = {
    "user_ip_iris": {
        "sql": f"{CLEAN_SQL_PATH}/user_ip_iris.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "user_ip_iris${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "month_log"},
    },
    "user_reco_iris": {
        "sql": f"{CLEAN_SQL_PATH}/user_reco_iris.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "user_reco_iris${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "month_log"},
    },
    "user_declared_iris": {
        "sql": f"{CLEAN_SQL_PATH}/user_declared_iris.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "user_declared_iris",
    },
    "applicative_database_titelive_gtl": {
        "sql": f"{CLEAN_SQL_PATH}/applicative_database/titelive_gtl.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_titelive_gtl",
    },

}


raw_tables = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH, BQ_DESTINATION_DATASET=BIGQUERY_RAW_DATASET
)
