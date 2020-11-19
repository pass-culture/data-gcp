import pytest
from google.cloud import bigquery

from bigquery.tests.queries_to_be_tested import define_simple_query_1, define_simple_query_2
from bigquery.tests.config import TEST_DATASET, GCP_PROJECT
from bigquery.tests.data import (
    SIMPLE_TABLE_1_INPUT, SIMPLE_TABLE_1_EXPECTED, SIMPLE_TABLE_2_EXPECTED,
    SIMPLE_TABLE_2_INPUT,
)
from bigquery.tests.utils import drop_dataset, create_dataset, drop_table, create_data, run_query, retrieve_data
from set_env import set_env_vars


@pytest.fixture(scope="module", autouse=True)
def prepare_bigquery():
    set_env_vars()
    pytest.bq_client = bigquery.Client()
    drop_dataset(client=pytest.bq_client, dataset=TEST_DATASET)
    create_dataset(client=pytest.bq_client, dataset=TEST_DATASET)
    yield
    drop_dataset(client=pytest.bq_client, dataset=TEST_DATASET)


@pytest.fixture()
def flush_dataset():
    yield
    dataset_id = f"{GCP_PROJECT}.{TEST_DATASET}"
    tables = pytest.bq_client.list_tables(dataset_id)
    for table in tables:
        drop_table(client=pytest.bq_client, dataset=TEST_DATASET, table=table.table_id)


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        ("simple_table_1", define_simple_query_1(dataset=TEST_DATASET), SIMPLE_TABLE_1_INPUT, SIMPLE_TABLE_1_EXPECTED),
        ("simple_table_2", define_simple_query_2(dataset=TEST_DATASET), SIMPLE_TABLE_2_INPUT, SIMPLE_TABLE_2_EXPECTED),
    ]
)
def test_create_queries(flush_dataset, table_name, query, input_data, expected):
    create_data(client=pytest.bq_client, dataset=TEST_DATASET, data=input_data)
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(client=pytest.bq_client, dataset=TEST_DATASET, table=table_name)
    assert sorted(output, key=lambda d: d["id"]) == sorted(expected, key=lambda d: d["id"])
