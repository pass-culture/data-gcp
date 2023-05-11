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
    "clean_iris_venues_in_shape": {
        "sql": f"{CLEAN_SQL_PATH}/iris_venues_in_shape.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "iris_venues_in_shape",
        "depends": ["venue"],
    },
    "clean_iris_venues_at_radius": {
        "sql": f"{CLEAN_SQL_PATH}/iris_venues_at_radius.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "iris_venues_at_radius",
        "params": {"iris_distance": 150000 if ENV_SHORT_NAME != "dev" else 20000},
        "depends": ["venue"],
    },
    "offer": {
        "sql": f"{CLEAN_SQL_PATH}/offer.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_offer",
    },
    "venue": {
        "sql": f"{CLEAN_SQL_PATH}/venue.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_venue",
    },
    "educational_institution": {
        "sql": f"{CLEAN_SQL_PATH}/educational_institution.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "applicative_database_educational_institution",
    },
    "user_beneficiary": {
        "sql": f"{CLEAN_SQL_PATH}/user_beneficiary.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "user_beneficiary",
    },
    "user_suspension": {
        "sql": f"{CLEAN_SQL_PATH}/user_suspension.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "user_suspension",
    },
    "bookable_offer": {
        "sql": f"{CLEAN_SQL_PATH}/bookable_offer.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "bookable_offer",
        "depends": ["offer", "available_stock_information"],
    },
    "available_stock_information": {
        "sql": f"{CLEAN_SQL_PATH}/available_stock_information.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "available_stock_information",
    },
    "cleaned_stock": {
        "sql": f"{CLEAN_SQL_PATH}/cleaned_stock.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "cleaned_stock",
    },
}


raw_tables = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH, BQ_DESTINATION_DATASET=BIGQUERY_RAW_DATASET
)

# Generate dictionnary for tables to copy from raw to clean.
def get_clean_tables_copy_dict():
    clean_tables_copy = {
        table: raw_tables[table]
        for table in raw_tables
        if table not in clean_tables.keys()
    }
    for (
        table
    ) in clean_tables_copy.keys():  # Update destination table to BIGQUERY_CLEAN_DATASET
        clean_tables_copy[table][
            "sql"
        ] = f"SELECT * FROM {BIGQUERY_RAW_DATASET}.applicative_database_{table}"
        clean_tables_copy[table]["destination_dataset"] = BIGQUERY_CLEAN_DATASET
    return clean_tables_copy
