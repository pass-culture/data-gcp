# Constants
from enum import StrEnum

TITELIVE_TOKEN_ENDPOINT = "https://login.epagine.fr/v1/login"
TITELIVE_BASE_URL = "https://catsearch.epagine.fr/v1"


class TITELIVE_CATEGORIES(StrEnum):
    LIVRE = "paper"
    MUSIQUE_ENREGISTREE = "music"
