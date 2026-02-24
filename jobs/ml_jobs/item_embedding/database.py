import pandas as pd
from loguru import logger


def load_parquet(input_parquet_filename: str) -> pd.DataFrame:
    """Load dataframe from a parquet file.

    Args:
        input_parquet_filename: Path to the parquet file containing item metadata on GCS
    Returns:
        DataFrame with item metadata
    """
    logger.info(f"Loading data from: {input_parquet_filename}")
    try:
        df = pd.read_parquet(input_parquet_filename)
        logger.info(f"Loaded {len(df)} items from {input_parquet_filename}")
        return df
    except Exception as e:
        logger.error(f"Error loading parquet file: {e}")
        raise


def upload_parquet(df: pd.DataFrame, output_parquet_filename: str) -> None:
    """Upload dataframe to a parquet file.

    Args:
        df: DataFrame containing item metadata and embeddings
        output_parquet_filename: Path to the output parquet file on GCS
    """
    logger.info(f"Uploading dataframe to: {output_parquet_filename}")
    try:
        df.to_parquet(output_parquet_filename, index=False)
        logger.info(f"Upload successful to {output_parquet_filename}")
    except Exception as e:
        logger.error(f"Error uploading parquet file: {e}")
        raise
