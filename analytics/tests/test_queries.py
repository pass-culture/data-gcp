import pytest
from google.cloud import bigquery

from analytics.tests.config import TEST_DATASET, GCP_PROJECT
from analytics.tests.data import (
    ENRICHED_OFFER_DATA_INPUT, ENRICHED_OFFER_DATA_EXPECTED, ENRICHED_USER_DATA_INPUT,
    ENRICHED_USER_DATA_EXPECTED, ENRICHED_VENUE_DATA_INPUT, ENRICHED_VENUE_DATA_EXPECTED,
    ENRICHED_OFFERER_DATA_INPUT, ENRICHED_OFFERER_DATA_EXPECTED,
)
from analytics.tests.utils import (
    drop_dataset,
    create_dataset,
    drop_table,
    create_data,
    run_query,
    retrieve_data,
    get_table_columns,
)
from dependencies.data_analytics.enriched_data.offer import define_enriched_offer_data_full_query
from dependencies.data_analytics.enriched_data.offerer import define_enriched_offerer_data_full_query
from dependencies.data_analytics.enriched_data.user import define_enriched_user_data_full_query
from dependencies.data_analytics.enriched_data.venue import define_enriched_venue_data_full_query
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
    ["table_name", "query", "input_data", "expected", "sorting_key"],
    [
        (
            "enriched_offer_data",
            define_enriched_offer_data_full_query(dataset=TEST_DATASET),
            ENRICHED_OFFER_DATA_INPUT,
            ENRICHED_OFFER_DATA_EXPECTED,
            "offer_id",
        )
    ],
)
def test_create_queries(flush_dataset, table_name, query, input_data, expected, sorting_key):
    create_data(client=pytest.bq_client, dataset=TEST_DATASET, data=input_data)
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name
    )
    assert sorted(output, key=lambda d: d[sorting_key]) == sorted(
        expected, key=lambda d: d[sorting_key]
    )


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_user_data",
            define_enriched_user_data_full_query(dataset=TEST_DATASET),
            ENRICHED_USER_DATA_INPUT,
            ENRICHED_USER_DATA_EXPECTED,
        ),
        (
            "enriched_venue_data",
            define_enriched_venue_data_full_query(dataset=TEST_DATASET),
            ENRICHED_VENUE_DATA_INPUT,
            ENRICHED_VENUE_DATA_EXPECTED,
        ),
        (
            "enriched_offerer_data",
            define_enriched_offerer_data_full_query(dataset=TEST_DATASET),
            ENRICHED_OFFERER_DATA_INPUT,
            ENRICHED_OFFERER_DATA_EXPECTED,
        ),
    ],
)
def test_create_queries_empty(flush_dataset, table_name, query, input_data, expected):
    create_data(client=pytest.bq_client, dataset=TEST_DATASET, data=input_data)
    run_query(client=pytest.bq_client, query=query)
    output = get_table_columns(client=pytest.bq_client, dataset=TEST_DATASET, table=table_name)
    assert output == expected
