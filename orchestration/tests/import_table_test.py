import unittest

from dependencies.import_analytics.import_tables import define_import_query


IMPORT_TABLES = [
    {"table_name": "provider", "excluded_fields": ["apiKey"]},
    {
        "table_name": "user",
        "excluded_fields": [
            "fistName",
            "lastName",
            "phoneNumber",
            "email",
            "publicName",
            "password",
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
            result = define_import_query(
                table_name,
                region="GCP_REGION",
                external_connection_id="EXTERNAL_CONNECTION_ID",
            )
            for f in excluded_fields:
                self.assertFalse(
                    f'"{f}"' in result,
                    f"Error in import_tables. Field {f} found in {table_name}",
                )
