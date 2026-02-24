"""
Script to partition the existing parquet file by offer_subcategory_id and venue_department_code.
This enables hive partitioning for massive query speedups.

Usage:
    python parquet_partioning.py --env dev
    python parquet_partioning.py --env prod --dry-run
"""

import os
import sys
from pathlib import Path

import polars as pl
import typer
from loguru import logger

GCP_PROJECT = os.getenv("GCP_PROJECT", "passculture-data-ehp")
env = os.getenv("env", "dev")


# Add app to path to import constants
sys.path.insert(0, str(Path(__file__).parent))

app = typer.Typer()


@app.command()
def partition_parquet(
    env: str = typer.Option("dev", help="Environment: dev,stg or prod"),
    dry_run: bool = typer.Option(
        False, help="Show what would be done without executing"
    ),
):
    """
    Partition existing parquet file for optimal query performance.
    """
    ENVIRONMENT = "prod" if env == "prod" else "ehp"
    PARQUET_FILE = f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/chatbot_encoded_offers_metadata_{env}/data-sorted.parquet"
    logger.info(f"Starting partitioning for environment: {env} ({ENVIRONMENT})")
    logger.info(f"Source file: {PARQUET_FILE}")

    # Define output path (replace data-sorted.parquet with partitioned/)
    output_base = PARQUET_FILE.replace("/data-sorted.parquet", "/partitioned")
    logger.info(f"Output location: {output_base}")

    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        logger.info(f"Would read from: {PARQUET_FILE}")
        logger.info(f"Would write to: {output_base}")
        logger.info("Partitioning by: offer_subcategory_id, venue_department_code")
        logger.info("Sorting within partitions by: item_id")
        return

    try:
        # Read source parquet
        logger.info("Reading source parquet file...")
        df = pl.read_parquet(PARQUET_FILE)
        logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")

        # Check required columns exist
        required_cols = ["offer_subcategory_id", "venue_department_code", "item_id"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            logger.error(f"Available columns: {df.columns}")
            raise ValueError(f"Missing columns: {missing}")

        # Sort for optimal performance within partitions
        logger.info("Sorting data...")
        df = df.sort(["offer_subcategory_id", "venue_department_code", "item_id"])

        # Log partition distribution
        partition_counts = (
            df.group_by(["offer_subcategory_id", "venue_department_code"])
            .agg(pl.count())
            .sort("count", descending=True)
        )
        logger.info(f"Will create {len(partition_counts)} partitions")
        logger.info(f"Top 5 largest partitions:\n{partition_counts.head(5)}")

        # Write with hive partitioning
        logger.info("Writing partitioned parquet...")
        df.write_parquet(
            output_base,
            partition_by=["offer_subcategory_id", "venue_department_code"],
            use_pyarrow=True,
            pyarrow_options={
                "compression": "snappy",
            },
        )

        logger.info("✅ Partitioning completed successfully!")
        logger.info(f"Update PARQUET_FILE constant to: {output_base}/**/*.parquet")

        # Provide update instructions
        logger.info("\n" + "=" * 60)
        logger.info("NEXT STEPS:")
        logger.info("1. Update app/constants.py:")
        logger.info(f'   PARQUET_FILE = "{output_base}/**/*.parquet"')
        logger.info("2. Restart your application")
        logger.info(
            "3. Queries with subcategory/department filters will be 10-20x faster!"
        )
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Partitioning failed: {e}")
        raise


if __name__ == "__main__":
    app()
