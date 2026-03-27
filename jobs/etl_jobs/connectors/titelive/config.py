"""Configuration for Titelive API connector."""

# API Endpoints
TITELIVE_BASE_URL = "https://catsearch.epagine.fr/v1"
TITELIVE_TOKEN_ENDPOINT = "https://login.epagine.fr/v1/login"

# API Configuration
RESPONSE_ENCODING = "utf-8"
EAN_SEPARATOR = "|"
MAX_SEARCH_RESULTS = 20_000
DEFAULT_RESULTS_PER_PAGE = 120

# EAN Batch Configuration
MAX_EANS_PER_REQUEST = 250  # API limit for /ean endpoint

# Token Configuration (from API probing report)
TOKEN_LIFETIME_SECONDS = 300  # 5 minutes
TOKEN_REFRESH_BUFFER_SECONDS = 30  # Refresh at 4.5 min

# Rate Limiting Configuration (from API probing report)
# Burst phase: 70 req/s for ~2000 requests (~28 seconds)
# Recovery phase: 15 second pause
# Sustained phase: 30 req/s
BURST_RATE = 70.0  # requests per second during burst
BURST_SIZE = 2000  # number of requests in burst phase
RECOVERY_TIME = 15.0  # seconds to pause after burst
SUSTAINED_RATE = 30.0  # requests per second after recovery

# Connection Pooling
POOL_CONNECTIONS = 10
POOL_MAXSIZE = 20
REQUEST_TIMEOUT = 30


def get_titelive_credentials(project_id: str) -> tuple[str, str]:
    """
    Fetch Titelive credentials from GCP Secret Manager.

    Args:
        project_id: GCP project ID

    Returns:
        Tuple of (username, password)
    """
    from utils.gcp import access_secret_data

    username = access_secret_data(project_id, "titelive_epagine_api_username")
    password = access_secret_data(project_id, "titelive_epagine_api_password")

    return username, password
