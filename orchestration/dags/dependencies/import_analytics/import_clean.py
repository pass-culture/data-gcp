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

clean_tables = {}


raw_tables = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH, BQ_DESTINATION_DATASET=BIGQUERY_RAW_DATASET
)
