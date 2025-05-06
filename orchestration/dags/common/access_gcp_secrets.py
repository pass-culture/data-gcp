import json
import secrets
import string
from typing import Optional, Union

from google.api_core.exceptions import AlreadyExists, NotFound, PermissionDenied
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


def access_secret_data(
    project_id: str,
    secret_id: str,
    default: Optional[Union[str, dict]] = None,
    as_dict: bool = False,
) -> Optional[Union[str, dict]]:
    """
    Access secret data from Google Cloud Secret Manager, with optional JSON deserialization.

    Args:
        project_id (str): The Google Cloud project ID.
        secret_id (str): The ID of the secret to access.
        default: The default value to return if the secret is not accessible.
        as_dict (bool): If True, attempt to deserialize the secret as JSON and return a dictionary.

    Returns:
        str or dict: The secret data as a string, or a dictionary if `as_dict` is True and the secret is valid JSON.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        try:
            response = client.access_secret_version(request={"name": name})
        except TypeError:
            response = client.access_secret_version(name)

        secret_data = response.payload.data.decode("UTF-8")

        # Attempt to deserialize JSON if requested
        if as_dict:
            try:
                return json.loads(secret_data)
            except json.JSONDecodeError:
                raise ValueError(f"Secret data for '{secret_id}' is not valid JSON.")

        return secret_data
    except (DefaultCredentialsError, PermissionDenied, NotFound):
        return default
    except Exception:
        return default


def write_secret_key(
    project_id: str, secret_id: str, secret_string: str, labels: Optional[dict] = None
) -> Optional[str]:
    """
    Write a string as secret data to Google Cloud Secret Manager.

    Args:
        project_id (str): The Google Cloud project ID.
        secret_id (str): The ID of the secret to write or create.
        secret_string (str): The string to store in the secret.
        labels (dict): Optional labels to assign to the secret.

    Returns:
        str: The name of the created/updated secret version, or None if an error occurred.
    """
    try:
        # Initialize the Secret Manager client
        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{project_id}"

        # Ensure the input is a string
        if not isinstance(secret_string, str):
            raise ValueError("secret_string must be a string")

        # Check if the secret exists
        try:
            client.get_secret(request={"name": f"{parent}/secrets/{secret_id}"})
        except NotFound:
            # Create the secret if it doesn't exist
            try:
                client.create_secret(
                    request={
                        "parent": parent,
                        "secret_id": secret_id,
                        "secret": {
                            "replication": {"automatic": {}},
                            "labels": labels or {},
                        },
                    }
                )
            except AlreadyExists:
                # The secret might have been created concurrently
                pass

        # Add the new secret version
        try:
            response = client.add_secret_version(
                request={
                    "parent": f"{parent}/secrets/{secret_id}",
                    "payload": {"data": secret_string.encode("UTF-8")},
                }
            )
            return response.name
        except PermissionDenied:
            return None

    except (DefaultCredentialsError, PermissionDenied, NotFound) as e:
        print(f"Error writing secret: {str(e)}")
        return None


def create_key_if_not_exists(
    project_id: str,
    secret_id: str,
    key_length: int = 32,
    secret_string: Optional[str] = None,
    labels: Optional[dict] = None,
) -> Optional[str]:
    """
    Write a string as secret data to Google Cloud Secret Manager only if it doesn't exist.

    Args:
        project_id (str): The Google Cloud project ID.
        secret_id (str): The ID of the secret to write or create.
        secret_string (str): The string to store in the secret.
        labels (dict): Optional labels to assign to the secret.

    Returns:
        tuple[bool, str | None]: A tuple containing:
            - bool: True if a new secret was written, False if it already existed
            - str | None: The secret value if exists, None if error occurred
    """
    if secret_string is None:
        assert key_length >= 16, "Key length must be >= 16 for security reason"
        secret_string = "".join(
            secrets.choice(string.digits) for _ in range(key_length)
        )
    try:
        # Try to access the existing secret
        existing_secret = access_secret_data(project_id, secret_id)

        if existing_secret is not None:
            return existing_secret

        # If no secret exists or access failed, try to write it
        secret_version = write_secret_key(project_id, secret_id, secret_string, labels)

        if secret_version:
            return secret_string

    except Exception as e:
        print(f"Error handling secret: {str(e)}")

    return None
