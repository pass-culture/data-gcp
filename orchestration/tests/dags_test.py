import unittest

from airflow.models import DagBag


class TestDags(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
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
        dag = self.dagbag.get_dag(dag_id="recommendation_cloud_sql_v38")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 50)

    def test_dump_scalingo_dag_is_loaded(self):
        # When
        dag = self.dagbag.get_dag(dag_id="dump_scalingo_v1")

        # Then
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 16)
