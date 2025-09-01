# Constants
import os
from enum import StrEnum

from src.utils.gcp import access_secret_data

TITELIVE_TOKEN_ENDPOINT = "https://login.epagine.fr/v1/login"
TITELIVE_BASE_URL = "https://catsearch.epagine.fr/v1"
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "passculture-data-dev")


TITELIVE_USERNAME = access_secret_data(
    GCP_PROJECT_ID,
    "titelive_epagine_api_username",
)
TITELIVE_PASSWORD = access_secret_data(
    GCP_PROJECT_ID,
    "titelive_epagine_api_password",
)


class TITELIVE_CATEGORIES(StrEnum):
    LIVRE = "paper"
    MUSIQUE_ENREGISTREE = "music"
