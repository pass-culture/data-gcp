import pandas as pd
import pyarrow as pa
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

    Embedding columns (lists of floats) are written with an explicit
    PyArrow schema using ``list_<float64>`` so that BigQuery loads them
    as ``ARRAY<FLOAT64>`` instead of a nested RECORD.

    Args:
        df: DataFrame containing item metadata and embeddings
        output_parquet_filename: Path to the output parquet file on GCS
    """
    # Build a PyArrow schema: list-of-float columns get pa.list_(pa.float64()),
    # other columns are inferred automatically.
    pa_fields = []
    for col in df.columns:
        if (
            df[col].dtype == object
            and len(df) > 0
            and isinstance(df[col].iloc[0], list)
        ):
            pa_fields.append(pa.field(col, pa.list_(pa.float64())))
        else:
            pa_fields.append(pa.field(col, pa.Array.from_pandas(df[col]).type))
    schema = pa.schema(pa_fields)

    logger.info(f"Uploading dataframe to: {output_parquet_filename}")

    df.to_parquet(
        output_parquet_filename,
        index=False,
        engine="pyarrow",
        schema=schema,
    )
    logger.info(f"Upload successful to {output_parquet_filename}")
