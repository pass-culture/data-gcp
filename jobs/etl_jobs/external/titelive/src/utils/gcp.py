import requests
from google.cloud import secretmanager, storage


def upload_image_to_gcs(
    storage_client: storage.Client, base_image_url: str, gcs_upload_url: str
):
    """
    Downloads an image from a URL and uploads it to Google Cloud Storage.

    Args:
        base_image_url (str): The URL of the image to download.
        gcs_upload_url (str): The full GCS path (e.g., gs://your-bucket/path/to/image.jpg).

    Returns:
        tuple: A tuple (success_boolean, original_base_image_url, message)
    """
    try:
        # 1. Download the image
        response = requests.get(base_image_url, stream=True, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

        # 2. Manage edge cases
        if not gcs_upload_url.startswith("gs://"):
            # GCS URL format: gs://bucket-name/path/to/blob
            return False, base_image_url, f"Invalid GCS URL format: {gcs_upload_url}"

        # 3. Extract bucket name and blob path from GCS URL
        parts = gcs_upload_url[len("gs://") :].split("/", 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # 4. Upload to GCS
        # Using a file-like object directly from requests response content
        blob.upload_from_file(
            response.raw, content_type=response.headers["Content-Type"]
        )

        print(f"Successfully uploaded {base_image_url} to {gcs_upload_url}")
        return True, base_image_url, "Success"

    except requests.exceptions.RequestException as e:
        return False, base_image_url, f"Network/Download error: {e}"
    except Exception as e:
        return False, base_image_url, f"GCS upload or other error: {e}"


def access_secret_data(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
