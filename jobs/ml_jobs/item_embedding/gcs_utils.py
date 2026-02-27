import pandas as pd
from loguru import logger


def load_parquet(
    input_parquet_filename: str, required_columns: list[str] | None = None
) -> pd.DataFrame:
    """Load dataframe from a parquet file.

    Args:
        input_parquet_filename: Path to the parquet file containing item metadata on GCS
        required_columns: Optional list of columns that must be present in the DataFrame

    Returns:
        DataFrame with item metadata

    Raises:
        FileNotFoundError: If the parquet file does not exist
        ValueError: If required columns are missing
    """
    logger.info(f"Loading data from: {input_parquet_filename}")
    df = pd.read_parquet(input_parquet_filename, engine="pyarrow")
    logger.info(f"Loaded {len(df)} items from {input_parquet_filename}")

    if required_columns:
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Input parquet is missing required columns: {', '.join(missing)}"
            )
    return df


def upload_parquet(df: pd.DataFrame, output_parquet_filename: str) -> None:
    """Upload dataframe to a parquet file.

    Args:
        df: DataFrame containing item metadata and embeddings
        output_parquet_filename: Path to the output parquet file on GCS
    """
    logger.info(f"Uploading dataframe to: {output_parquet_filename}")

    df.to_parquet(
        output_parquet_filename,
        index=False,
        engine="pyarrow",
    )
    logger.info(f"Upload successful to {output_parquet_filename}")
