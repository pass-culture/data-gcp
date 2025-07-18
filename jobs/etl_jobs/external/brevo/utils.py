import logging
import os
import time
from functools import wraps

from brevo_python.rest import ApiException
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_TMP_DATASET = f"tmp_{ENV_SHORT_NAME}"


def rate_limiter(calls: int, period: int, log_waits: bool = True):
    """
    Improved rate limiter that respects API limits and handles 429 errors.

    Args:
        calls: Maximum number of calls per period
        period: Time period in seconds
        log_waits: Whether to log waiting times
    """
    interval = period / calls

    def decorator(func):
        last_call = [0]  # Use list to allow modification in nested function
        call_count = [0]

        @wraps(func)
        def wrapper(*args, **kwargs):
            call_count[0] += 1
            current_time = time.time()

            # Calculate when the next call should be allowed
            time_since_last = current_time - last_call[0]
            min_interval = interval

            # If we haven't waited long enough, sleep
            if time_since_last < min_interval:
                wait_time = min_interval - time_since_last
                if log_waits:
                    logging.info(
                        f"Rate limit: waiting {wait_time:.1f}s (call #{call_count[0]}, limit: {calls}/{period}s)"
                    )
                time.sleep(wait_time)

            # Update last call time
            last_call[0] = time.time()

            # Try the API call with retry logic for 429 errors
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        logging.info(f"API call succeeded on retry {attempt}")
                    return result

                except ApiException as e:
                    if e.status == 429:  # Rate limit error
                        # Parse rate limit headers if available
                        reset_time = None

                        if hasattr(e, "http_resp") and hasattr(e.http_resp, "headers"):
                            headers = e.http_resp.headers
                            reset_time = headers.get("x-sib-ratelimit-reset")
                            remaining = headers.get("x-sib-ratelimit-remaining")
                            limit = headers.get("x-sib-ratelimit-limit")

                            logging.warning(
                                f"Rate limit exceeded (attempt {attempt + 1}/{max_retries})"
                            )
                            logging.warning(
                                f"  Limit: {limit}, Remaining: {remaining}, Reset in: {reset_time}s"
                            )

                        if attempt < max_retries - 1:  # Not the last attempt
                            if reset_time:
                                # Wait for the reset time plus a small buffer
                                wait_time = int(reset_time) + 10
                                logging.info(
                                    f"  Waiting {wait_time}s for rate limit reset..."
                                )
                                time.sleep(wait_time)
                            else:
                                # Exponential backoff if no reset time
                                wait_time = (2**attempt) * 60  # 1min, 2min, 4min
                                logging.info(
                                    f"  Exponential backoff: waiting {wait_time}s..."
                                )
                                time.sleep(wait_time)
                        else:
                            # Last attempt failed, re-raise the exception
                            logging.error(
                                f"Rate limit exceeded after {max_retries} attempts"
                            )
                            raise
                    else:
                        # Non-rate-limit API error, re-raise immediately
                        logging.error(f"API error (status {e.status}): {e}")
                        raise

                except Exception as e:
                    # Non-API error, re-raise immediately
                    logging.error(f"Unexpected error in rate-limited function: {e}")
                    raise

            # This should never be reached
            return None

        return wrapper

    return decorator


def adaptive_rate_limiter(initial_calls: int = 280, period: int = 3600):
    """
    Adaptive rate limiter that adjusts based on API response headers.
    """

    def decorator(func):
        current_limit = [initial_calls]
        interval = [period / initial_calls]
        last_call = [0]
        call_count = [0]

        @wraps(func)
        def wrapper(*args, **kwargs):
            call_count[0] += 1
            current_time = time.time()

            # Rate limiting logic
            time_since_last = current_time - last_call[0]
            if time_since_last < interval[0]:
                wait_time = interval[0] - time_since_last
                logging.info(
                    f"Adaptive rate limit: waiting {wait_time:.1f}s (call #{call_count[0]})"
                )
                time.sleep(wait_time)

            last_call[0] = time.time()

            try:
                result = func(*args, **kwargs)

                # Check if we got rate limit headers and adjust if needed
                if hasattr(result, "http_resp") and hasattr(
                    result.http_resp, "headers"
                ):
                    headers = result.http_resp.headers
                    api_limit = headers.get("x-sib-ratelimit-limit")

                    if api_limit and int(api_limit) != current_limit[0]:
                        current_limit[0] = int(api_limit)
                        interval[0] = period / current_limit[0]
                        logging.info(
                            f"Adjusted rate limit to {current_limit[0]} calls per {period}s"
                        )

                return result

            except ApiException as e:
                if e.status == 429:
                    # Handle rate limit as before
                    if hasattr(e, "http_resp") and hasattr(e.http_resp, "headers"):
                        headers = e.http_resp.headers
                        reset_time = headers.get("x-sib-ratelimit-reset")
                        if reset_time:
                            wait_time = int(reset_time) + 10
                            logging.warning(
                                f"Rate limit hit, waiting {wait_time}s for reset..."
                            )
                            time.sleep(wait_time)
                            # Retry the call
                            return func(*args, **kwargs)
                raise

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
