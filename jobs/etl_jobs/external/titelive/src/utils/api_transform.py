"""Transform API responses to simplified 2-column format for BigQuery."""

import json

import pandas as pd

from src.utils.logging import get_logger

logger = get_logger(__name__)


def transform_api_response(api_response: dict) -> pd.DataFrame:
    """
    Transform Titelive API response to simplified 2-column DataFrame.

    Output schema:
        - ean (STRING): Product EAN from article.gencod
        - json_raw (STRING): Full article data as JSON string

    Args:
        api_response: Raw API response dictionary with structure:
            {"result": [
                {"id": 123, "article": {"1": {...}, "2": {...}}},
                {"id": 456, "article": [{...}, {...}]}
            ]}
            OR
            {"result": {"0": {...}, "1": {...}}}

    Returns:
        DataFrame with columns: ean, json_raw

    Raises:
        ValueError: If API response format is invalid
    """
    if "result" not in api_response:
        if "oeuvre" in api_response:
            api_response = {"result": api_response["oeuvre"]}
            # logger.info(api_response)
        else:
            msg = "Invalid API response format: missing 'result' key"
            raise ValueError(msg)

    results = api_response["result"]

    # Handle both result formats: list or dict
    if isinstance(results, dict):
        # Convert dict values to list (format 2)
        results = list(results.values())
    elif not isinstance(results, list):
        msg = "Invalid API response format: 'result' must be a list or dict"
        raise ValueError(msg)

    # Flatten nested structure: result[].article.{number}
    rows = []
    for result_item in results:
        if not isinstance(result_item, dict):
            continue

        articles_data = result_item.get("article", {})

        # Handle both article formats: dict with numbered keys or list
        if isinstance(articles_data, dict):
            # Format 1: dict with numbered keys {"1": {...}, "2": {...}}
            articles_list = list(articles_data.values())
        elif isinstance(articles_data, list):
            # Format 2: list of articles [{...}, {...}]
            articles_list = articles_data
        else:
            continue

        # Find first valid article with ean
        ean = None
        for article in articles_list:
            if not isinstance(article, dict):
                continue

            # Extract required field
            ean = article.get("gencod")

            if ean:
                break

        # Skip result_item if no valid article found
        if not ean:
            continue

        # Store entire result_item as JSON
        json_raw = json.dumps(result_item, ensure_ascii=False)

        rows.append(
            {
                "ean": str(ean),
                "json_raw": json_raw,
            }
        )

    if not rows:
        logger.warning("API response contains no valid articles")
        return pd.DataFrame(columns=["ean", "json_raw"])

    df = pd.DataFrame(rows)

    # Remove duplicates based on ean
    df = df.drop_duplicates(subset=["ean"])

    logger.info(f"Transformed API response to {len(df)} rows")
    return df
