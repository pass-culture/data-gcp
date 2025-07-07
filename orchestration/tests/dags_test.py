import unittest
from datetime import timedelta
from unittest import mock

import pandas as pd
from jobs.crons import SCHEDULE_DICT

from airflow.models import DagBag

DAG_ID_LIST = list(SCHEDULE_DICT.keys())


class TestDags(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = timedelta(seconds=2)

    def setUp(self):
        with (
            mock.patch(
                "common.bigquery_client.BigQueryClient.query"
            ) as bigquery_mocker,
            mock.patch(
                "common.access_gcp_secrets.access_secret_data"
            ) as access_secret_mocker,
        ):

            def bigquery_client(query):
                return (
                    pd.DataFrame(
                        {"f0_": [pd.to_datetime(1490195805, unit="s", utc=True)]}
                    )
                    if "SELECT max(visit_last_action_time) FROM" in query
                    else pd.DataFrame({0: [0]})
                )

            access_secret_mocker.return_value = "{}"
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

    def test_dags_are_loaded(self):
        for dag_id in DAG_ID_LIST:
            dag = self.dagbag.get_dag(dag_id=dag_id)
            self.assertIsNotNone(dag, f"DAG {dag_id} should exist.")
