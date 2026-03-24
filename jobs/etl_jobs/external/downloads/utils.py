import os
from datetime import timedelta

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

KEY_ID = "CR65Y9UN25"
ISSUER_ID = "591abf9a-10b5-4c9e-a70b-62bf336008d4"

PROJECT_NAME = os.environ.get("PROJECT_NAME", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"


class SecretStr:
    """String wrapper that refuses to reveal its value in repr/str/logging."""

    __slots__ = ("_value",)

    def __init__(self, value: str):
        self._value = value

    def get_secret_value(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "SecretStr('**********')"

    def __str__(self) -> str:
        return "**********"

    def __len__(self) -> int:
        return len(self._value)


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


PRIVATE_KEY = SecretStr(
    access_secret_data(PROJECT_NAME, f"api-apple-{ENV_SHORT_NAME}") or ""
)
BUCKET_NAME = access_secret_data(
    PROJECT_NAME, f"downloads_bucket_name_{ENV_SHORT_NAME}"
)


def get_last_month(today):
    current_month_first_day = today.replace(day=1)
    last_month = (current_month_first_day - timedelta(days=1)).replace(day=1)
    return last_month
