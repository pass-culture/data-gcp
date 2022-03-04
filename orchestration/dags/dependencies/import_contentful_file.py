import pandas as pd
from dependencies.config import (
    DATA_GCS_BUCKET_NAME,
)


def load_from_csv_contentful_file():
    return pd.read_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/contentful_home/contentful_home.csv"
    )
