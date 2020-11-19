import pytest
from google.cloud import bigquery

from bigquery.tests.queries_to_be_tested import define_simple_query_1, define_simple_query_2
from bigquery.tests.config import BIGQUERY_SCHEMAS, TEST_DATASET, GCP_REGION, GCP_PROJECT
from bigquery.tests.data import (
    SIMPLE_TABLE_1_INPUT, SIMPLE_TABLE_1_EXPECTED, SIMPLE_TABLE_2_EXPECTED,
    SIMPLE_TABLE_2_INPUT,
)
from set_env import set_env_vars


def drop_dataset(client, dataset):
    dataset_id = f"{GCP_PROJECT}.{dataset}"
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def create_dataset(client, dataset):
    dataset_id = f"{GCP_PROJECT}.{dataset}"
    bq_dataset = bigquery.Dataset(dataset_id)
    bq_dataset.location = GCP_REGION
    client.create_dataset(bq_dataset, timeout=30)


def drop_table(client, dataset, table):
    table_id = f"{GCP_PROJECT}.{dataset}.{table}"
    client.delete_table(table_id, not_found_ok=True)


def create_table(client, dataset, table):
    table_id = f"{GCP_PROJECT}.{dataset}.{table}"
    schema = [bigquery.SchemaField(col_name, col_type) for col_name, col_type in BIGQUERY_SCHEMAS[table].items()]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)


def insert_rows(client, dataset, table, rows):
    # table_id = f"{GCP_PROJECT}.{dataset}.{table}"
    # client.insert_rows_json(table_id, rows)  # does not work... (something to do with the async creation operation)
    job_config = bigquery.QueryJobConfig()
    job_config.destination = f"{GCP_PROJECT}.{dataset}.{table}"
    job_config.write_disposition = "WRITE_APPEND"
    for row in rows:
        fields = ", ".join([f"CAST('{val}' AS {BIGQUERY_SCHEMAS[table][col]}) AS {col}" for col, val in row.items()])
        query = f"SELECT {fields};"
        query_job = client.query(query=query,  job_config=job_config)
        query_job.result()


def create_data(client, dataset, data):
    for table_name, table_rows in data.items():
        create_table(client=client, dataset=dataset, table=table_name)
        insert_rows(client=client, dataset=dataset, table=table_name, rows=table_rows)


def run_query(client, query):
    query_job = client.query(query=query)
    query_job.result()


def retrieve_data(client, dataset, table):
    table_id = f"{GCP_PROJECT}.{dataset}.{table}"
    rows_iter = client.list_rows(table_id)
    return [dict(row.items()) for row in rows_iter]


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
