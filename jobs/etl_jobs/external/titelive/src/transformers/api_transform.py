"""Transform API responses to simplified 3-column format for BigQuery."""

import json
from datetime import datetime

import pandas as pd

from src.utils.logging import get_logger

logger = get_logger(__name__)


def transform_api_response(api_response: dict) -> pd.DataFrame:
    """
    Transform Titelive API response to simplified 3-column DataFrame.

    Output schema:
        - ean (STRING): Product EAN from article.gencod
        - datemodification (DATE): Modification date from article.datemodification
        - json_raw (STRING): Full article data as JSON string

    Args:
        api_response: Raw API response dictionary with structure:
            {"result": {"1": {..., "article": [...]}, "2": {...}}}

    Returns:
        DataFrame with columns: ean, datemodification, json_raw

    Raises:
        ValueError: If API response format is invalid
    """
    if "result" not in api_response:
        msg = "Invalid API response format: missing 'result' key"
        raise ValueError(msg)

    results = api_response["result"]
    if not isinstance(results, dict):
        msg = "Invalid API response format: 'result' must be a dictionary"
        raise ValueError(msg)

    # Flatten nested structure: result.{number}.article[]
    rows = []
    for product_key, product_data in results.items():
        if not isinstance(product_data, dict):
            continue

        articles = product_data.get("article", [])
        if not isinstance(articles, list):
            continue

        for article in articles:
            if not isinstance(article, dict):
                continue

            # Extract required fields
            ean = article.get("gencod")
            date_str = article.get("datemodification")

            # Skip if missing required fields
            if not ean or not date_str:
                logger.warning(
                    f"Skipping article in product {product_key}: "
                    f"missing ean or datemodification"
                )
                continue

            # Convert date from DD/MM/YYYY to DATE
            try:
                date_obj = datetime.strptime(date_str, "%d/%m/%Y").date()
            except ValueError:
                logger.warning(
                    f"Skipping article {ean}: invalid date format '{date_str}'"
                )
                continue

            # Store entire article as JSON
            json_raw = json.dumps(article, ensure_ascii=False)

            rows.append(
                {
                    "ean": str(ean),
                    "datemodification": date_obj,
                    "json_raw": json_raw,
                }
            )

    if not rows:
        logger.warning("API response contains no valid articles")
        return pd.DataFrame(columns=["ean", "datemodification", "json_raw"])

    df = pd.DataFrame(rows)

    # Remove duplicates based on ean
    df = df.drop_duplicates(subset=["ean"])

    logger.info(f"Transformed API response to {len(df)} rows")
    return df
