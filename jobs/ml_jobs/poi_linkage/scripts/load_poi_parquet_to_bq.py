"""One-off load of the POI Parquet file (see export_poi_parquet.py) into BigQuery.

Usage:
    uv run python scripts/load_poi_parquet_to_bq.py
    uv run python scripts/load_poi_parquet_to_bq.py --write-disposition WRITE_APPEND
"""

import typer
from google.cloud import bigquery

from src.constants import POI_TABLE
from src.utils.gcp import get_bigquery_client


def main(
    parquet_path: str = typer.Option("data/output/base_lieux_culturels_ouverts.parquet"),
    table: str = typer.Option(POI_TABLE, help="BigQuery table id, without project prefix."),
    write_disposition: str = typer.Option(
        "WRITE_TRUNCATE",
        help="WRITE_TRUNCATE (replace), WRITE_APPEND, or WRITE_EMPTY (fail if table has rows).",
    ),
) -> None:
    client = get_bigquery_client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )
    with open(parquet_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table, job_config=job_config)
    load_job.result()

    destination = client.get_table(table)
    typer.echo(f"Loaded {destination.num_rows} rows into {table}")


if __name__ == "__main__":
    typer.run(main)
