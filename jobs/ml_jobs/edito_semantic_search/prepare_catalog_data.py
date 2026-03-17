import os
from pathlib import Path

import polars as pl
import pyarrow.dataset as ds
import typer
from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger
from pyarrow.fs import GcsFileSystem

load_dotenv()

app = typer.Typer()


@app.command()
def prepare_catalog_data(
    env: str = typer.Option("dev", help="Environment: dev, stg, or prod"),
    partition: bool = typer.Option(
        True, help="Run partitioning after creating parquet"
    ),
    partition_cols: list[str] = typer.Option(
        ["offer_subcategory_id", "venue_department_code"],
        "--partition-col",
        "-p",
        help="Columns to partition by. Pass multiple times for nested partitions",
    ),
):
    """
    Run BigQuery query, export to parquet in GCS, and optionally partition it.
    """
    # Setup parameters based on environment
    gcp_project = os.getenv("GCP_PROJECT", "passculture-data-ehp")
    gcp_env = "prod" if env == "prod" else "ehp"

    analytics_dataset = f"analytics_{env}"
    sandbox_dataset = f"sandbox_{env}"
    mlfeat_dataset = f"ml_feat_{env}"
    output_table = "chatbot_edito_search_db_offers"
    output_path = (
        f"gs://mlflow-bucket-{gcp_env}/streamlit_data/chatbot_edito/offers_{env}/"
    )

    logger.info(f"Starting catalog data preparation for environment: {env}")
    logger.info(f"Output path: {output_path}")

    # 1. Read and format SQL query
    sql_file = Path(__file__).parent / "sql" / "chatbot_catalog_offers.sql"
    with open(sql_file) as f:
        sql = f.read()

    query = sql.format(
        project=gcp_project,
        analytics_dataset=analytics_dataset,
        sandbox_dataset=sandbox_dataset,
        mlfeat_dataset=mlfeat_dataset,
    )

    logger.info("Executing BigQuery query...")

    # 2. Run BigQuery query and export to parquet
    client = bigquery.Client(project=gcp_project)

    # Use EXPORT DATA statement for direct parquet export
    export_query = f"""
    EXPORT DATA OPTIONS(
        uri='{output_path}/data-*.parquet',
        format='PARQUET',
        overwrite=true
    ) AS
    {query}
    """

    query_job = client.query(export_query)
    _ = query_job.result()

    logger.success(f"Query completed. Parquet files exported to {output_path}")
    logger.info(f"Total rows processed: {query_job.total_bytes_processed:,} bytes")

    # 3. Run partitioning if requested
    if partition:
        logger.info("Starting partitioning...")
        logger.info(f"Target partition columns: {partition_cols}")

        output_base = f"gs://mlflow-bucket-{gcp_env}/streamlit_data/chatbot_edito/offers_{env}_partitioned"

        try:
            # Read source parquet
            logger.info(f"Reading source parquet: {output_path}")
            df = pl.read_parquet(output_path)
            logger.info(f"Loaded {len(df):,} rows.")
            logger.info(f"Columns available in source: {df.columns}")

            # Check required columns
            required_cols = list(partition_cols) + ["item_id"]
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                raise ValueError(
                    f"Missing required columns: {missing}. Available: {df.columns}"
                )

            # Sort for optimal performance
            logger.info("Sorting data...")
            df = df.sort(required_cols)

            # Write directly to GCS using PyArrow (HIVE FORMAT)
            logger.info(f"Writing partitioned parquet directly to {output_base}...")
            gcs = GcsFileSystem()
            gcs_target_path = output_base.replace("gs://", "")

            table = df.to_arrow()
            hive_partitioning = ds.partitioning(
                table.select(partition_cols).schema,
                flavor="hive",
            )

            ds.write_dataset(
                table,
                base_dir=gcs_target_path,
                filesystem=gcs,
                format="parquet",
                partitioning=hive_partitioning,
                existing_data_behavior="overwrite_or_ignore",
            )
            logger.info("✅ Direct GCS write completed!")

            # Verify on GCS
            logger.info("Verifying files on GCS...")
            row_count = (
                pl.scan_parquet(f"{output_base}/**/*.parquet", hive_partitioning=True)
                .select(pl.len())
                .collect()
                .item()
            )
            logger.info(f"✅ Verified {row_count:,} rows on GCS.")
            logger.success("Partitioning completed!")

        except Exception as e:
            logger.error(f"Partitioning failed: {e}")
            raise
    else:
        logger.info("Skipping partitioning (--no-partition was set)")

    logger.success("Catalog data preparation complete!")


if __name__ == "__main__":
    app()
