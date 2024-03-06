import pytest
from data_analytics.utils.json import approx_equal
from jinja2 import Environment, FileSystemLoader
from google.cloud import bigquery
from data_analytics.config import GCP_PROJECT, TEST_DATASET
from data_analytics.data import (
    ENRICHED_STOCK_DATA_EXPECTED,
    ENRICHED_STOCK_DATA_INPUT,
)

from data_analytics.utils.gcp import (
    create_data,
    create_dataset,
    drop_dataset,
    drop_table,
    run_query,
)
from common.macros import create_humanize_id_function
from dependencies.import_analytics.import_analytics import export_tables
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)


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
    ["table_name", "input_data", "expected", "sorting_key"],
    [
        (
            "enriched_stock_data",
            ENRICHED_STOCK_DATA_INPUT,
            ENRICHED_STOCK_DATA_EXPECTED,
            "stock_id",
        ),
    ],
)
def test_create_queries(flush_dataset, table_name, input_data, expected, sorting_key):
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
    )
    table_params = export_tables[table_name]
    environment = Environment(loader=FileSystemLoader("dags/"))
    template = environment.get_template(table_params["sql"])

    sql = template.render(
        bigquery_analytics_dataset=TEST_DATASET,
        bigquery_clean_dataset=TEST_DATASET,
        bigquery_raw_dataset=TEST_DATASET,
        create_humanize_id_function=create_humanize_id_function,
    )

    output = run_query(client=pytest.bq_client, query=sql)

    output = approx_equal(
        sorted(output, key=lambda d: d[sorting_key]),
        precision=2,
    )
    expected = approx_equal(sorted(expected, key=lambda d: d[sorting_key]), precision=2)

    assert len(output) == len(expected)
    for x, y in zip(output, expected):
        assert dict(x) == dict(y)
