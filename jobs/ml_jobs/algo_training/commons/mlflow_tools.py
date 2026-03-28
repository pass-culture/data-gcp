import datetime
import json
import os

import google.auth
import mlflow
from google.cloud import iam_credentials_v1

from commons.constants import MLFLOW_URI, SA_ACCOUNT


def generate_jwt_payload(service_account_email: str, resource_url: str) -> str:
    """Generates JWT payload for service account.

    Creates a properly formatted JWT payload with standard claims (iss, sub,
    aud, iat, exp) needed for IAP authentication.

    Args:
        service_account_email (str): Specifies the service account that the
        JWT is created for.
        resource_url (str): Specifies the scope of the JWT, the URL that the
        JWT will be allowed to access.

    Returns:
        str: JSON string containing the JWT payload with properly formatted
        claims.
    """
    # Create current time and expiration time (3 hour later) in UTC
    iat = datetime.datetime.now(tz=datetime.UTC)
    exp = iat + datetime.timedelta(seconds=3 * 3600)

    payload = {
        "iss": service_account_email,
        "sub": service_account_email,
        "aud": resource_url,
        "iat": int(iat.timestamp()),
        "exp": int(exp.timestamp()),
    }

    return json.dumps(payload)


def sign_jwt(target_sa: str, resource_url: str) -> str:
    """Signs JWT payload using ADC and IAM credentials API.

    Uses Google Cloud's IAM Credentials API to sign a JWT. This requires the
    caller to have iap.webServiceVersions.accessViaIap permission on the
    target service account.

    Args:
        target_sa (str): Service Account JWT is being created for.
            iap.webServiceVersions.accessViaIap permission is required.
        resource_url (str): Audience of the JWT and scope of the JWT token.
            This is the URL of the IAP-secured application.

    Returns:
        str: A signed JWT that can be used to access IAP-secured applications.
            Use in Authorization header as: 'Bearer <signed_jwt>'
    """
    source_credentials, _ = google.auth.default()
    iam_client = iam_credentials_v1.IAMCredentialsClient(credentials=source_credentials)
    name = iam_client.service_account_path("-", target_sa)
    payload = generate_jwt_payload(target_sa, resource_url)
    response = iam_client.sign_jwt(name=name, payload=payload)

    return response.signed_jwt


def connect_remote_mlflow() -> None:
    """Connect to remote MLflow tracking server using signed JWT for authentication."""
    signed_jwt = sign_jwt(
        target_sa=SA_ACCOUNT,
        resource_url=MLFLOW_URI
        + "*",  # add * wildcard to get a token that works for all MLflow API endpoints
        # under the base URI
    )
    os.environ["MLFLOW_TRACKING_TOKEN"] = signed_jwt
    mlflow.set_tracking_uri(MLFLOW_URI)


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment
