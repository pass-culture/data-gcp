import pytest
from google.cloud import bigquery
from data_analytics.config import GCP_PROJECT, TEST_DATASET

from data_analytics.utils.gcp import (
    create_dataset,
    drop_dataset,
    drop_table,
)
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
