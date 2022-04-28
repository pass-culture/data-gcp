import unittest

from dependencies.data_analytics.import_tables import define_import_query


class TestImportTables(unittest.TestCase):
    def test_import_tables_does_not_import_not_anonymized_column_for_provider(self):
        # Given
        table = "provider"

        # When
        result = define_import_query(
            table, region="GCP_REGION", external_connection_id="EXTERNAL_CONNECTION_ID"
        )

<<<<<<< HEAD:orchestration/tests/import_table_test.py
        # Then
        assert not '"apiKey"' in result

    def test_import_tables_does_not_import_not_anonymized_column_for_user(self):
        # Given
        table = "user"

        # When
        result = define_import_query(
            table, region="GCP_REGION", external_connection_id="EXTERNAL_CONNECTION_ID"
        )

        # Then
        assert not '"fistName"' in result
        assert not '"lastName"' in result
        assert not '"phoneNumber"' in result
        assert not '"email"' in result
        assert not '"publicName"' in result
        assert not '"password"' in result
        assert not '"validationToken"' in result
        assert not '"resetPasswordToken"' in result

    def test_import_tables_does_not_import_not_anonymized_column_for_bank_information(
        self,
    ):
        # Given
        table = "bank_information"

        # When
        result = define_import_query(
            table, region="GCP_REGION", external_connection_id="EXTERNAL_CONNECTION_ID"
        )

        # Then
        assert not '"iban"' in result
        assert not '"bic"' in result

    def test_import_tables_does_not_import_not_anonymized_column_for_payment(self):
        # Given
        table = "payment"

        # When
        result = define_import_query(
            table, region="GCP_REGION", external_connection_id="EXTERNAL_CONNECTION_ID"
        )

        # Then
        assert not '"iban"' in result
        assert not '"bic"' in result

    def test_import_tables_does_not_import_not_anonymized_column_for_booking(self):
        # Given
        table = "booking"

        # When
        result = define_import_query(
            table, region="GCP_REGION", external_connection_id="EXTERNAL_CONNECTION_ID"
        )

        # Then
        assert not '"token"' in result
=======
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
>>>>>>> 8788153 (fix tests):orchestration/tests/import_tables_test.py
