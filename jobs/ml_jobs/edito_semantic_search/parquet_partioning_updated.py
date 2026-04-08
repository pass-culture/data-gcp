import sys
from pathlib import Path

import polars as pl
import pyarrow.dataset as ds
import typer
from loguru import logger
from pyarrow.fs import GcsFileSystem

# Add app to path to import constants
sys.path.insert(0, str(Path(__file__).parent))

app = typer.Typer()


@app.command()
def partition_parquet(
    env: str = typer.Option("dev", help="Environment: dev, stg, or prod"),
    partition_cols: list[str] = typer.Option(
        ["offer_subcategory_id", "venue_department_code"],
        "--partition-col",
        "-p",
        help="Columns to partition by. Pass multiple times for nested partitions (e.g., -p col1 -p col2)",
    ),
    analyze_only: bool = typer.Option(
        False, help="Analyze the dataset to recommend if the chosen columns are efficient"
    ),
    dry_run: bool = typer.Option(
        False, help="Show what would be done without executing"
    ),
):
    """
    Partition existing parquet file for optimal query performance.
    """
    gcp_env = "prod" if env == "prod" else "ehp"
    parquet_file = f"gs://mlflow-bucket-{gcp_env}/streamlit_data/chatbot_edito/offers_{env}/"
    output_base = f"gs://mlflow-bucket-{gcp_env}/streamlit_data/chatbot_edito/offers_{env}_partitioned"

    logger.info(f"Starting partitioning for environment: {env} ({gcp_env})")
    logger.info(f"Target partition columns: {partition_cols}")

    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        return

    try:
        # 1. Read source parquet (unpartitioned files, so no hive_partitioning needed)
        logger.info(f"Reading source parquet: {parquet_file}")
        df = pl.read_parquet(parquet_file)
        logger.info(f"Loaded {len(df):,} rows.")
        logger.info(f"Columns available in source: {df.columns}")
        
        # 2. Check required columns
        required_cols = list(partition_cols) + ["item_id"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(
                f"Missing required columns: {missing}. Available: {df.columns}"
            )

        # 3. Analyze Partition Efficiency (New Feature)
        if analyze_only:
            logger.info("--- PARTITION EFFICIENCY ANALYSIS ---")
            for col in partition_cols:
                n_unique = df.select(pl.col(col).n_unique()).item()
                logger.info(f"Column '{col}' has {n_unique} unique values.")
                if n_unique > 5000:
                    logger.warning(f"⚠️ '{col}' has high cardinality! This may cause the 'small files' problem.")
                elif n_unique < 2:
                    logger.warning(f"⚠️ '{col}' has very low cardinality. Partitioning won't help performance much.")
            
            # Show skew/distribution
            partition_counts = df.group_by(partition_cols).len().sort("len", descending=True)
            total_partitions = len(partition_counts)
            avg_rows = len(df) / total_partitions
            
            logger.info(f"Total partitions that will be created: {total_partitions}")
            logger.info(f"Average rows per partition: {avg_rows:,.0f}")
            logger.info(f"Top 5 largest partitions:\n{partition_counts.head(5)}")
            logger.info("Exiting early because --analyze-only was passed.")
            return

        # 4. Sort for optimal performance
        logger.info("Sorting data...")
        df = df.sort(required_cols)

        # 5. Write directly to GCS using PyArrow (HIVE FORMAT)
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

        # 6. Verify on GCS
        logger.info("Verifying files on GCS...")
        row_count = (
            pl.scan_parquet(f"{output_base}/**/*.parquet", hive_partitioning=True)
            .select(pl.len())
            .collect()
            .item()
        )
        logger.info(f"✅ Verified {row_count:,} rows on GCS.")

    except Exception as e:
        logger.error(f"Partitioning failed: {e}")
        raise

if __name__ == "__main__":
    app()