import pytest
from data_analytics.utils.json import approx_equal
from google.cloud import bigquery
from data_analytics.config import GCP_PROJECT, TEST_DATASET, TEST_TABLE_PREFIX
from data_analytics.data import (
    ENRICHED_BOOKING_DATA_EXPECTED,
    ENRICHED_BOOKING_DATA_INPUT,
    ENRICHED_OFFER_DATA_EXPECTED,
    ENRICHED_OFFER_DATA_INPUT,
    ENRICHED_COLLECTIVE_OFFER_DATA_EXPECTED,
    ENRICHED_COLLECTIVE_OFFER_DATA_INPUT,
    ENRICHED_OFFERER_DATA_EXPECTED,
    ENRICHED_OFFERER_DATA_INPUT,
    ENRICHED_STOCK_DATA_EXPECTED,
    ENRICHED_STOCK_DATA_INPUT,
    ENRICHED_USER_DATA_EXPECTED,
    ENRICHED_USER_DATA_INPUT,
    ENRICHED_VENUE_DATA_EXPECTED,
    ENRICHED_VENUE_DATA_INPUT,
    TEST_TABLE_PREFIX,
)

from data_analytics.utils.gcp import (
    create_data,
    create_dataset,
    drop_dataset,
    drop_table,
    retrieve_data,
    run_query,
)
from dependencies.data_analytics.enriched_data.booking import (
    define_enriched_booking_data_full_query,
)
from dependencies.data_analytics.enriched_data.offer import (
    define_enriched_offer_data_full_query,
)
from dependencies.data_analytics.enriched_data.collective_offer import (
    define_enriched_collective_offer_data_full_query,
)
from dependencies.data_analytics.enriched_data.offerer import (
    define_enriched_offerer_data_full_query,
)
from dependencies.data_analytics.enriched_data.stock import (
    define_enriched_stock_data_full_query,
)
from dependencies.data_analytics.enriched_data.user import (
    define_enriched_user_data_full_query,
)
from dependencies.data_analytics.enriched_data.venue import (
    define_enriched_venue_data_full_query,
)


@pytest.fixture(scope="module", autouse=True)
def prepare_bigquery():

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
            "enriched_offerer_data",
            define_enriched_offerer_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_OFFERER_DATA_INPUT,
            ENRICHED_OFFERER_DATA_EXPECTED,
            "offerer_id",
        ),
        (
            "enriched_venue_data",
            define_enriched_venue_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_VENUE_DATA_INPUT,
            ENRICHED_VENUE_DATA_EXPECTED,
            "venue_id",
        ),
        (
            "enriched_booking_data",
            define_enriched_booking_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_BOOKING_DATA_INPUT,
            ENRICHED_BOOKING_DATA_EXPECTED,
            "booking_id",
        ),
        (
            "enriched_user_data",
            define_enriched_user_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_USER_DATA_INPUT,
            ENRICHED_USER_DATA_EXPECTED,
            "user_id",
        ),
        (
            "enriched_offer_data",
            define_enriched_offer_data_full_query(
                analytics_dataset=TEST_DATASET,
                clean_dataset=TEST_DATASET,
                table_prefix=TEST_TABLE_PREFIX,
            ),
            ENRICHED_OFFER_DATA_INPUT,
            ENRICHED_OFFER_DATA_EXPECTED,
            "offer_id",
        ),
        (
            "enriched_collective_offer_data",
            define_enriched_collective_offer_data_full_query(
                analytics_dataset=TEST_DATASET,
                clean_dataset=TEST_DATASET,
                table_prefix=TEST_TABLE_PREFIX,
            ),
            ENRICHED_COLLECTIVE_OFFER_DATA_INPUT,
            ENRICHED_COLLECTIVE_OFFER_DATA_EXPECTED,
            "collective_offer_id",
        ),
        (
            "enriched_stock_data",
            define_enriched_stock_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_STOCK_DATA_INPUT,
            ENRICHED_STOCK_DATA_EXPECTED,
            "stock_id",
        ),
    ],
)
def test_create_queries(
    flush_dataset, table_name, query, input_data, expected, sorting_key
):
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )
    output = approx_equal(
        sorted(output, key=lambda d: d[sorting_key]),
        precision=2,
    )
    expected = approx_equal(sorted(expected, key=lambda d: d[sorting_key]), precision=2)

    assert len(output) == len(expected)
    for x, y in zip(output, expected):
        assert x == y
