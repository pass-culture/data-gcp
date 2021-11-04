import unittest
from datetime import timedelta
from unittest import mock

import pandas as pd
from airflow.models import DagBag


class TestDags(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = timedelta(seconds=2)

    def setUp(self):
        with mock.patch(
            "dependencies.bigquery_client.BigQueryClient.query"
        ) as bigquery_mocker, mock.patch(
            "dependencies.matomo_client.MatomoClient.query"
        ) as matomo_mocker, mock.patch(
            "dependencies.access_gcp_secrets.access_secret_data"
        ) as access_secret_mocker:

            def bigquery_client(query):
                return (
                    pd.DataFrame(
                        {"f0_": [pd.to_datetime(1490195805, unit="s", utc=True)]}
                    )
                    if "SELECT max(visit_last_action_time) FROM" in query
                    else pd.DataFrame({0: [0]})
                )

            access_secret_mocker.return_value = "{}"
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

    def test_recommendation_cloud_sql_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="recommendation_cloud_sql_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 73)

    def test_create_ab_testing_table_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="export_cloudsql_tables_to_bigquery_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 9)

    def test_dump_matomo_history_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_matomo_history_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 19)

    def test_import_matomo_refresh_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_matomo_refresh_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 41)

    def test_import_data_analytics_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_data_analytics_v6")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 85)

    def test_import_firebase_data_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_firebase_data_v3")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 8)

    def test_import_typeform_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_typeform_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 11)

    def test_import_addresses_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_addresses_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 8)

    def test_import_dms_subscription_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_dms_subscriptions")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 8)

    def test_import_siren_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="import_siren_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 5)

    def test_compute_monitoring_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="compute_monitoring")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 9)
