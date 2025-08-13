import os

import pandas as pd
from loguru import logger

from utils.gcs_utils import get_last_date_from_bucket


def load_wikidata(wiki_base_path: str, wiki_file_name: str) -> pd.DataFrame:
    latest_path = os.path.join(
        wiki_base_path, get_last_date_from_bucket(wiki_base_path), wiki_file_name
    )
    logger.info(f"Loading Wikidata from {latest_path}")

    return pd.read_parquet(latest_path)
