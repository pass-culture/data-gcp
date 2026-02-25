import os
import sys
from pathlib import Path

import polars as pl
import pyarrow.dataset as ds
from pyarrow.fs import GcsFileSystem
import typer
from loguru import logger

# Add app to path to import constants
sys.path.insert(0, str(Path(__file__).parent))

app = typer.Typer()

@app.command()
def partition_parquet(
    env: str = typer.Option("dev", help="Environment: dev, stg, or prod"),
    dry_run: bool = typer.Option(False, help="Show what would be done without executing"),
):
    """
    Partition existing parquet file for optimal query performance.
    """
    gcp_env = "prod" if env == "prod" else "ehp"
    parquet_file = f"gs://mlflow-bucket-{gcp_env}/streamlit_data/chatbot_edito/chatbot_encoded_offers_metadata_{env}/data-sorted.parquet"
    output_base = parquet_file.replace("/data-sorted.parquet", "/partitioned")

    logger.info(f"Starting partitioning for environment: {env} ({gcp_env})")
    
    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        logger.info(f"Source: {parquet_file}")
        logger.info(f"Destination: {output_base}")
        return

    try:
        # 1. Read source parquet
        logger.info(f"Reading source parquet: {parquet_file}")
        df = pl.read_parquet(parquet_file)
        logger.info(f"Loaded {len(df):,} rows.")

        # 2. Check required columns
        partition_cols = ["offer_subcategory_id", "venue_department_code"]
        required_cols = partition_cols + ["item_id"]
        
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}. Available: {df.columns}")

        # 3. Sort for optimal performance
        logger.info("Sorting data...")
        df = df.sort(required_cols)

        # Log partition distribution 
        partition_counts = (
            df.group_by(partition_cols)
            .len()
            .sort("len", descending=True)
        )
        logger.info(f"Will create {len(partition_counts)} partitions. Top 5:\n{partition_counts.head(5)}")

        # 4. Write directly to GCS using PyArrow (HIVE FORMAT)
        logger.info(f"Writing partitioned parquet directly to {output_base}...")
        
        # Initialize the GCS File System (auto-detects VM credentials)
        gcs = GcsFileSystem()
        
        # PyArrow's GcsFileSystem expects the path without the 'gs://' prefix
        gcs_target_path = output_base.replace("gs://", "")
        
        table = df.to_arrow()
        
        # THE FIX: Create an explicit Hive partitioning schema
        hive_partitioning = ds.partitioning(
            table.select(partition_cols).schema,  # <-- Swap .schema and .select
            flavor="hive"
        )
        
        ds.write_dataset(
            table,
            base_dir=gcs_target_path,
            filesystem=gcs,
            format="parquet",
            partitioning=hive_partitioning,  # <-- Use the explicit Hive schema here
            existing_data_behavior="overwrite_or_ignore"
        )
        logger.info("✅ Direct GCS write completed!")

        # 5. Verify on GCS
        logger.info("Verifying files on GCS...")
        row_count = pl.scan_parquet(f"{output_base}/**/*.parquet", hive_partitioning=True).select(pl.len()).collect().item()
        logger.info(f"✅ Verified {row_count:,} rows on GCS.")

        # Provide update instructions
        logger.info("\n" + "=" * 60)
        logger.info("NEXT STEPS:")
        logger.info(f'1. Update app/constants.py: PARQUET_FILE = "{output_base}/**/*.parquet"')
        logger.info("2. Restart your application")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Partitioning failed: {e}")
        raise

if __name__ == "__main__":
    app()