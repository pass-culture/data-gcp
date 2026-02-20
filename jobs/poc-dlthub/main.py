import io

import dlt
import pyarrow.csv as pa_csv
from dlt.sources.helpers.requests.retry import Client
from google.cloud import bigquery
from loguru import logger
from pyarrow import RecordBatch, Table

from const import (
    API_URL,
    DATASET_NAME,
    DEFAULT_BATCH_SIZE,
    DEFAULT_DESTINATION_TABLE_NAME,
    GCP_PROJECT,
    PYARROW_TABLE_SCHEMA,
    QUERY,
)


_rest_client_session = Client().session


def call_geocoding_search_batch_api(batch: RecordBatch) -> Table:
    """
    Fetch geocoded data from the Geopf API for a batch of rows.

    Args:
        batch: PyArrow RecordBatch containing the rows to geocode

    Returns:
        PyArrow Table with geocoded data
    """
    bytes_buffer = io.BytesIO()
    pa_csv.write_csv(batch, bytes_buffer)
    bytes_buffer.seek(0)

    files = {"data": ("addresses.csv", bytes_buffer, "text/csv")}
    response = _rest_client_session.post(API_URL, files=files, data={"columns": "user_full_address"})

    return pa_csv.read_csv(
        io.BytesIO(response.content),
        convert_options=pa_csv.ConvertOptions(
            column_types=PYARROW_TABLE_SCHEMA, null_values=[""], strings_can_be_null=True
        ),
    )


def run_pipeline(table_name: str = DEFAULT_DESTINATION_TABLE_NAME, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    """
    Run the geocoding pipeline with batch processing.

    Args:
        table_name: Name of the table in DuckDB (default: "geocoded_addresses")
        batch_size: Number of rows to process per batch (default: 10)
    """
    bq_client = bigquery.Client(project=GCP_PROJECT)
    query_job = bq_client.query(QUERY)
    results = query_job.result(page_size=batch_size)

    pipeline = dlt.pipeline(pipeline_name="geopf_geocoding", destination="duckdb", dataset_name=DATASET_NAME)

    @dlt.resource(write_disposition="replace", name=table_name)
    def geocoded_addresses() -> Table:
        for arrow_batch in results.to_arrow_iterable():
            logger.info(f"Processing batch with {arrow_batch.num_rows} rows")
            yield call_geocoding_search_batch_api(arrow_batch)

    pipeline.run(geocoded_addresses())

    logger.success(f"Pipeline completed successfully! {results.total_rows} total rows")


if __name__ == "__main__":
    run_pipeline()
