import json
from datetime import datetime

import pandas as pd
import requests

from src.constants import (
    TITELIVE_BASE_URL,
    TITELIVE_CATEGORIES,
    TITELIVE_TOKEN_ENDPOINT,
)
from src.env_vars import TITELIVE_IDENTIFIER, TITELIVE_PASSWORD

TOKEN = None
RESULTS_PER_PAGE = 120
BASE_TIMEOUT = 30
RESPONSE_ENCODING = "utf-8"
MAX_RESPONSES = 1e6


def get_titelive_token() -> str:
    """
    Returns the token from Titelive API
    """
    # Get configuration from environment variables or config

    if not all([TITELIVE_TOKEN_ENDPOINT, TITELIVE_IDENTIFIER, TITELIVE_PASSWORD]):
        raise ValueError(
            "Missing required environment variables: "
            "TITELIVE_IDENTIFIER, TITELIVE_PASSWORD"
        )

    # Construct the URL
    url = f"{TITELIVE_TOKEN_ENDPOINT}/{TITELIVE_IDENTIFIER}/token"

    # Prepare the request
    headers = {
        "Content-Type": "application/json",
    }

    body = {"password": TITELIVE_PASSWORD}

    try:
        # Make the POST request
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Extract the token from the response
        response_data = response.json()
        token = response_data.get("token")

        if not token:
            raise ValueError("No token found in response")

        return token

    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        raise
    except (KeyError, ValueError) as e:
        print(f"Error processing response: {e}")
        raise


def _get_valid_token() -> str:
    """
    Get a valid token, refreshing if necessary
    """
    global TOKEN
    if TOKEN is None:
        TOKEN = get_titelive_token()
    return TOKEN


def _refresh_token() -> str:
    """
    Force refresh the token
    """
    global TOKEN
    TOKEN = get_titelive_token()
    return TOKEN


def fetch_get_request(get_url: str, headers: dict, params: dict | None = None):
    if params is None:
        params = {}

    try:
        response = requests.get(
            get_url, headers=headers, params=params, timeout=BASE_TIMEOUT
        )

        if response.status_code == 401:
            print(f"Token expired for {get_url}, fetching a new one.")
            token = _refresh_token()
            headers["Authorization"] = f"Bearer {token}"
            response = requests.get(get_url, headers=headers, timeout=BASE_TIMEOUT)

        response.raise_for_status()

        # Ensure proper UTF-8 encoding (titelive API)
        response.encoding = RESPONSE_ENCODING
        return response.json()

    except requests.exceptions.RequestException as e:
        # For other request exceptions, don't retry
        raise requests.exceptions.RequestException(
            f"Request failed for {get_url}: {e}"
        ) from e

    except ValueError as e:
        raise ValueError(f"Error processing response for {get_url}: {e}") from e

    except Exception as e:
        raise Exception(f"Unexpected error for {get_url}: {e}") from e


def get_metadata_from_ean(ean: str) -> dict:
    """
    Get metadata for a single EAN with improved error handling and rate limiting
    """
    get_url = f"{TITELIVE_BASE_URL}/ean/{ean}"
    token = _get_valid_token()
    headers = {"Authorization": f"Bearer {token}"}

    return fetch_get_request(get_url, headers)


def get_modified_offers(
    offer_category: TITELIVE_CATEGORIES, min_modified_date: datetime
) -> pd.DataFrame:
    """
    Get modified offers from Titelive API using search endpoint
    """

    # Format the date as DD/MM/YYYY
    formatted_date = min_modified_date.strftime("%d/%m/%Y")

    # Construct the search URL with required parameters
    search_url = f"{TITELIVE_BASE_URL}/search"
    token = _get_valid_token()
    headers = {"Authorization": f"Bearer {token}"}
    page = 0
    params = {
        "base": offer_category,
        "dateminm": formatted_date,
        "nombre": str(RESULTS_PER_PAGE),
        "page": str(page),
    }

    # Iterate over the requests
    results = []
    while True and page < (MAX_RESPONSES // RESULTS_PER_PAGE):
        response_data = fetch_get_request(
            search_url, headers, {**params, "page": str(page)}
        )

        # Extract the results from the API response
        if isinstance(response_data, dict):
            fetched_results = response_data.get("result", [])
        else:
            fetched_results = []

        results.extend(fetched_results)
        page += 1

        if fetched_results == [] or len(fetched_results) < RESULTS_PER_PAGE:
            break

    return pd.DataFrame(
        {"ean": result["id"], "data": json.dumps(result, ensure_ascii=False)}
        for result in results
    ).set_index("ean")
