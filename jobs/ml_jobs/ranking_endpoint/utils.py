import datetime
import json
import os
import subprocess
import time

import google.auth
import mlflow
import pandas as pd
from google.cloud import iam_credentials_v1

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
SA_ACCOUNT = (
    f"algo-training-{ENV_SHORT_NAME}@passculture-data-ehp.iam.gserviceaccount.com"
)
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)


def save_experiment(experiment_name, model_name, serving_container, run_id):
    log_results = {
        "execution_date": datetime.datetime.now().isoformat(),
        "experiment_name": experiment_name,
        "model_name": model_name,
        "model_type": "custom",
        "run_id": run_id,
        "run_start_time": int(time.time() * 1000.0),
        "run_end_time": int(time.time() * 1000.0),
        "artifact_uri": None,
        "serving_container": serving_container,
    }
    pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
        project_id=f"{GCP_PROJECT_ID}",
        if_exists="append",
    )


def deploy_container(serving_container):
    command = f"sh ./deploy_to_docker_registery.sh {serving_container}"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


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
    # Create current time and expiration time (1 hour later) in UTC
    iat = datetime.datetime.now(tz=datetime.UTC)
    exp = iat + datetime.timedelta(seconds=3600)

    # Convert datetime objects to numeric timestamps (seconds since epoch)
    # as required by JWT standard (RFC 7519)
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
    # Get default credentials from environment or application credentials
    source_credentials, _ = google.auth.default()

    # Initialize IAM credentials client with source credentials
    iam_client = iam_credentials_v1.IAMCredentialsClient(credentials=source_credentials)

    # Generate the service account resource name.
    # Project should always be "-".
    # Replacing the wildcard character with a project ID is invalid.
    name = iam_client.service_account_path("-", target_sa)

    # Create and sign the JWT payload
    payload = generate_jwt_payload(target_sa, resource_url)

    # Sign the JWT using the IAM credentials API
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
