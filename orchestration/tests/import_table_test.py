import unittest

from dependencies.applicative_database.import_applicative_database import (
    get_tables_config_dict,
    RAW_SQL_PATH,
)
from common.config import DAG_FOLDER

IMPORT_TABLES = [
    {"table_name": "provider", "excluded_fields": ["apiKey"]},
    {
        "table_name": "user",
        "excluded_fields": [
            "fistName",
            "lastName",
            "phoneNumber",
            "email",
            "password",
            "validationToken",
            "resetPasswordToken",
        ],
    },
    {"table_name": "bank_information", "excluded_fields": ["iban", "bic"]},
    {"table_name": "payment", "excluded_fields": ["iban", "bic"]},
    {"table_name": "booking", "excluded_fields": ["token"]},
]


class TestImportTables(unittest.TestCase):
    def test_import_tables_does_not_import_not_anonymized_columns(self):
        for params in IMPORT_TABLES:
            table_name = params["table_name"]
            excluded_fields = params["excluded_fields"]

            result_path = get_tables_config_dict(
                DAG_FOLDER + "/" + RAW_SQL_PATH, "BIGQUERY_DATASET"
            )[table_name]["sql"]
            with open(result_path, "r") as file:
                result_query = file.readlines()
            for f in excluded_fields:
                self.assertFalse(
                    f'"{f}"' in result_query,
                    f"Error in import_tables. Field {f} found in {table_name}",
                )
