import os
import time
from functools import wraps

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_TMP_DATASET = f"tmp_{ENV_SHORT_NAME}"


def rate_limiter(calls: int, period: int):
    """Custom rate limiter decorator that ensures calls are evenly spaced within a period.

    Args:
        calls (int): Maximum number of calls allowed in the period
        period (int): Time period in seconds

    Returns:
        Function: Decorated function with rate limiting
    """
    # Calculate time between calls to space them evenly
    time_between_calls = period / calls

    # Store the last call time
    last_call_time = [0]  # Using list for mutable reference

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.time()
            elapsed = current_time - last_call_time[0]

            # If not enough time has passed since the last call
            if elapsed < time_between_calls:
                wait_time = time_between_calls - elapsed
                time.sleep(wait_time)

            # Update the last call time and execute the function
            last_call_time[0] = time.time()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


campaigns_histo_schema = {
    "campaign_id": "INTEGER",
    "campaign_utm": "STRING",
    "campaign_name": "STRING",
    "campaign_target": "STRING",
    "campaign_sent_date": "STRING",
    "share_link": "STRING",
    "update_date": "DATETIME",
    "audience_size": "INTEGER",
    "open_number": "INTEGER",
    "unsubscriptions": "INTEGER",
}

transactional_histo_schema = {
    "template": "INTEGER",
    "tag": "STRING",
    "email": "STRING",
    "event_date": "DATE",
    "target": "STRING",
    "delivered_count": "INTEGER",
    "opened_count": "INTEGER",
    "unsubscribed_count": "INTEGER",
}
