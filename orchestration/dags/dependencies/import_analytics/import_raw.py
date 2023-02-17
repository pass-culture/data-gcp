import os

RAW_SQL_PATH = f"dependencies/import_analytics/sql/raw"


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
