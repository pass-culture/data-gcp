"""Transform API responses to simplified 2-column format for BigQuery."""

import json
from datetime import datetime

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

        # Normalize result_item to have article as list format
        result_item["article"] = articles_list

        # Store normalized result_item as JSON
        json_raw = json.dumps(result_item, ensure_ascii=False)

        rows.append(
            {
                "ean": str(ean),
                "json_raw": json_raw,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["ean", "json_raw"])

    df = pd.DataFrame(rows)

    # Remove duplicates based on ean
    df = df.drop_duplicates(subset=["ean"])

    return df


def extract_gencods_from_search_response(
    api_response: dict, from_date: str
) -> tuple[list[str], dict]:
    """
    Extract unique gencod values from a search API response with date filtering.

    This function is used to extract EANs from /search endpoint results,
    which are then used to fetch detailed product data via /ean endpoint.

    Filters articles based on their datemodification field:
    - Articles without datemodification are included
    - Articles with invalid datemodification are included
    - Articles with datemodification >= from_date are included
    - Articles with datemodification < from_date are excluded

    Note: Duplicates (from articles without/invalid datemodification, or from
    API boundary overlaps) are handled by the final deduplication step in
    run_incremental.

    Args:
        api_response: Search API response with structure:
            {"result": [
                {"article": {"1": {"gencod": "X", "datemodification": "DD/MM/YYYY"}}},
                ...
            ]}
        from_date: Date filter in DD/MM/YYYY format.
            Only articles with datemodification >= from_date will be included.
            Articles without/invalid datemodification are also included.
            Duplicates are handled by final deduplication step.

    Returns:
        Tuple of (unique_gencods, filter_stats) where filter_stats contains:
            - total_articles: Total articles seen
            - filtered_count: Articles filtered by date
            - filtered_samples: List of (gencod, datemodification) for filtered items
            - no_date_count: Articles without datemodification field

    Raises:
        ValueError: If API response format is invalid or from_date format is invalid
    """
    if "result" not in api_response:
        msg = "Invalid API response format: missing 'result' key"
        raise ValueError(msg)

    results = api_response["result"]

    # Handle both result formats: list or dict
    if isinstance(results, dict):
        results = list(results.values())
    elif not isinstance(results, list):
        msg = "Invalid API response format: 'result' must be a list or dict"
        raise ValueError(msg)

    # Parse from_date (required)
    try:
        from_date_obj = datetime.strptime(from_date, "%d/%m/%Y")
    except ValueError as e:
        msg = f"Invalid from_date format: {from_date}. Expected DD/MM/YYYY."
        raise ValueError(msg) from e

    gencods = []
    total_articles = 0
    filtered_count = 0
    filtered_samples = []  # (gencod, datemodification) tuples
    no_date_count = 0

    for result_item in results:
        if not isinstance(result_item, dict):
            continue

        articles_data = result_item.get("article", {})

        # Handle both article formats: dict with numbered keys or list
        if isinstance(articles_data, dict):
            # Format: {"1": {...}, "2": {...}}
            articles_list = list(articles_data.values())
        elif isinstance(articles_data, list):
            # Format: [{...}, {...}]
            articles_list = articles_data
        else:
            continue

        # Extract gencod values from articles, applying date filter
        for article in articles_list:
            if not isinstance(article, dict):
                continue

            total_articles += 1
            gencod = article.get("gencod")

            # Apply date filter
            datemodification = article.get("datemodification")

            # Track articles without datemodification
            if datemodification is None:
                no_date_count += 1
                if gencod:
                    gencods.append(str(gencod))
                continue

            try:
                article_date = datetime.strptime(datemodification, "%d/%m/%Y")

                # Skip articles modified before from_date
                if article_date < from_date_obj:
                    filtered_count += 1
                    # Keep sample of filtered items (max 10)
                    if len(filtered_samples) < 10 and gencod:
                        filtered_samples.append((str(gencod), datemodification))
                    continue
            except ValueError:
                # Include articles with invalid date format
                logger.warning(
                    f"Invalid datemodification format for article with "
                    f"gencod {gencod}: {datemodification}. Including anyway."
                )

            if gencod:
                gencods.append(str(gencod))

    # Return unique gencods and filter stats
    unique_gencods = list(set(gencods))
    filter_stats = {
        "total_articles": total_articles,
        "filtered_count": filtered_count,
        "filtered_samples": filtered_samples,
        "no_date_count": no_date_count,
    }
    return unique_gencods, filter_stats
