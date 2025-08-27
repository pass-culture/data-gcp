from datetime import datetime

import requests

from src.constants import TITELIVE_BASE_URL, TITELIVE_TOKEN_ENDPOINT
from src.env_vars import TITELIVE_IDENTIFIER, TITELIVE_PASSWORD

TOKEN = None


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


def get_metadata_from_ean(ean: str) -> dict:
    """
    Get metadata for a single EAN with improved error handling and rate limiting
    """
    get_url = f"{TITELIVE_BASE_URL}/ean/{ean}"
    token = _get_valid_token()
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(get_url, headers=headers, timeout=30)

        if response.status_code == 401:
            print(f"Token expired for EAN {ean}, fetching a new one.")
            token = _refresh_token()
            headers["Authorization"] = f"Bearer {token}"
            response = requests.get(get_url, headers=headers, timeout=30)

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        # For other request exceptions, don't retry
        raise requests.exceptions.RequestException(
            f"Request failed for EAN {ean}: {e}"
        ) from e

    except ValueError as e:
        raise ValueError(f"Error processing response for EAN {ean}: {e}") from e

    except Exception as e:
        raise Exception(f"Unexpected error for EAN {ean}: {e}") from e


def get_modified_offers(offer_category: str, min_modified_date: datetime) -> dict:
    """
    Get modified offers from Titelive API using search endpoint
    """
    # Format the date as DD/MM/YYYY
    formatted_date = min_modified_date.strftime("%d/%m/%Y")

    # Construct the search URL with required parameters
    search_url = f"{TITELIVE_BASE_URL}/search"
    params = {"base": "paper", "dateminm": formatted_date, "nombre": "120", "page": "1"}

    token = _get_valid_token()
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(search_url, headers=headers, params=params, timeout=30)

        if response.status_code == 401:
            print("Token expired for search request, fetching a new one.")
            token = _refresh_token()
            headers["Authorization"] = f"Bearer {token}"
            response = requests.get(
                search_url, headers=headers, params=params, timeout=30
            )

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        # For other request exceptions, don't retry
        raise requests.exceptions.RequestException(f"Search request failed: {e}") from e

    except ValueError as e:
        raise ValueError(f"Error processing search response: {e}") from e

    except Exception as e:
        raise Exception(f"Unexpected error in search request: {e}") from e
