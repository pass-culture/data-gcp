import gcsfs
import pandas as pd
from loguru import logger


def list_parquet_files(gcs_path: str) -> list[str]:
    """List all Parquet files matching the given path.
    Args:
        gcs_path: GCS path
    Returns:
        List of GCS paths to Parquet files starting with gs://
    Raises:
        ValueError: If the input path is not a valid GCS path.
        FileNotFoundError: If no files are found matching the path.
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    fs = gcsfs.GCSFileSystem()
    l_files = fs.glob(gcs_path)
    if not l_files:
        raise FileNotFoundError(f"No files found for path: {gcs_path}")
    return [f"gs://{filename}" for filename in l_files]


def load_parquet_file(
    parquet_filename: str,
    vectors: list,
) -> pd.DataFrame:
    """Load a Parquet file and validate required columns.

    Accepts a single parquet filename, returns a DataFrame object.
    Supports local or GCS paths.

    Args:
        parquet_filename: Local or GCS (``gs://…``) path — file, directory, or glob.
        vectors: List of vector configurations used to determine required columns.

    Returns:
        pd.DataFrame containing the loaded data.

    Raises:
        ValueError: If required columns are missing from the DataFrame.
    """
    logger.info(f"Loading data from: {parquet_filename}")

    df = pd.read_parquet(parquet_filename)

    _validate_parquet_file(df, vectors)

    logger.info(
        f"Loaded {len(df)} items from {parquet_filename}, and validated required columns for embedding"
    )
    return df


def _validate_parquet_file(df: pd.DataFrame, vectors: list) -> None:
    """Validate that a DataFrame contains all required columns.

    Args:
        df: DataFrame to validate.
        vectors: List of vector configurations.

    Raises:
        ValueError: If any required column is missing from the DataFrame.
    """
    required_columns = list(
        set(
            ["item_id", "content_hash"]
            + [feature for vector in vectors for feature in vector.features]
        )
    )
    available = set(df.columns)
    missing = [c for c in required_columns if c not in available]
    if missing:
        raise ValueError(f"DataFrame is missing required columns: {', '.join(missing)}")
