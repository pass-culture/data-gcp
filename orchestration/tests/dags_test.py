import unittest
from unittest import mock

from airflow.models import DagBag
import pandas as pd


class TestDags(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        with mock.patch(
            "dependencies.bigquery_client.BigQueryClient.query"
        ) as bigquery_mocker, mock.patch(
            "dependencies.matomo_client.MatomoClient.query"
        ) as matomo_mocker:

            def bigquery_client(query):
                return (
                    pd.DataFrame(
                        {"f0_": [pd.to_datetime(1490195805, unit="s", utc=True)]}
                    )
                    if "SELECT max(visit_last_action_time) FROM" in query
                    else pd.DataFrame({0: [0]})
                )

            matomo_mocker.return_value = [[0]]
            bigquery_mocker.side_effect = bigquery_client
            self.dagbag = DagBag(include_examples=False)

    def test_dag_import_no_error(self):
        # Then
        self.assertFalse(
            len(self.dagbag.import_errors),
            "There should be no DAG failures. Got: {}".format(
                self.dagbag.import_errors
            ),
        )

    def test_dag_import_is_fast(self):
        # Given
        stats = self.dagbag.dagbag_stats
        slow_files = filter(lambda d: d.duration > self.LOAD_SECOND_THRESHOLD, stats)
        res = ", ".join(map(lambda d: d.file[1:], slow_files))

        # When / Then
        self.assertEqual(
            0,
            len(list(slow_files)),
            "The following files take more than {threshold}s to load: {res}".format(
                threshold=self.LOAD_SECOND_THRESHOLD, res=res
            ),
        )

    def test_restore_data_analytics_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="restore_data_analytics_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 21)

    def test_recommendation_cloud_sql_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="recommendation_cloud_sql_v42")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 51)

    def test_dump_prod_from_scalingo_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="dump_prod_from_scalingo_v2")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 162)

    def test_create_ab_testing_table_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="export_cloudsql_tables_to_bigquery_v3")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 5)

    def test_dump_matomo_history_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="dump_scalingo_matomo_history_v6")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 50)

    def test_dump_matomo_refresh_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="dump_scalingo_matomo_refresh_v4")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 21)

    def test_import_data_analytics_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_data_analytics_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 23)

    def test_archive_database_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="archive_database_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 3)

    def test_restore_scalingo_from_vm_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="restore_prod_from_vm_export_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 4)
