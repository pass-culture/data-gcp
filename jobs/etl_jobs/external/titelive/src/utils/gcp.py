import requests
from google.cloud import secretmanager, storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _get_session():
    """Create a requests session with connection pooling and retries."""
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    # Configure adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,  # Number of connection pools
        pool_maxsize=20,  # Max connections per pool
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


# Global session for connection reuse across all uploads
_session = _get_session()


def upload_image_to_gcs(
    storage_client: storage.Client, base_image_url: str, gcs_upload_url: str
):
    """
    Downloads an image from a URL and uploads it to Google Cloud Storage.

    Args:
        storage_client: GCS client instance
        base_image_url (str): The URL of the image to download.
        gcs_upload_url (str): The full GCS path (e.g., gs://your-bucket/path/to/image.jpg).

    Returns:
        tuple: A tuple (success_boolean, original_base_image_url, message)
    """
    try:
        # 1. Download the image using shared session
        response = _session.get(
            base_image_url,
            timeout=(10, 30),  # (connect_timeout, read_timeout)
            headers={"User-Agent": "TiteliveETL/1.0"},
        )
        response.raise_for_status()

        # 2. Manage edge cases
        if not gcs_upload_url.startswith("gs://"):
            return False, base_image_url, f"Invalid GCS URL format: {gcs_upload_url}"

        # 3. Extract bucket name and blob path from GCS URL
        parts = gcs_upload_url[len("gs://") :].split("/", 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # 4. Upload to GCS - use response.content for better performance
        content_type = response.headers.get("Content-Type", "image/jpeg")
        blob.upload_from_string(response.content, content_type=content_type)

        return True, base_image_url, "Success"

    except requests.exceptions.Timeout as e:
        return False, base_image_url, f"Timeout error: {e}"
    except requests.exceptions.ConnectionError as e:
        return False, base_image_url, f"Connection error: {e}"
    except requests.exceptions.RequestException as e:
        return False, base_image_url, f"Network/Download error: {e}"
    except Exception as e:
        return False, base_image_url, f"GCS upload or other error: {e}"


def access_secret_data(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
