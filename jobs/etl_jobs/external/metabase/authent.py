import datetime
import json

import google.auth

from google.cloud import iam_credentials_v1


def _generate_jwt_payload(service_account_email: str, resource_url: str) -> str:
    """Generates JWT payload for service account.

    Creates a properly formatted JWT payload with standard claims (iss, sub,
    aud, iat, exp) needed for IAP authentication.
    Warning: the JWT token expires after 1 hour (3600 seconds)
    and needs to be regenerated after that.


    Args:
        service_account_email (str): Specifies the service account that the
        JWT is created for.
        resource_url (str): Specifies the scope of the JWT, the URL that the
        JWT will be allowed to access.
        SA must have the role "Service Account Token Creator" on itself.

    Returns:
        str: JSON string containing the JWT payload with properly formatted
        claims.
    """
    # Create current time and expiration time (1 hour later) in UTC
    iat = datetime.datetime.now(tz=datetime.UTC)
    exp = iat + datetime.timedelta(seconds=3600)

    payload = {
        "iss": service_account_email,
        "sub": service_account_email,
        "aud": resource_url,
        "iat": int(iat.timestamp()),
        "exp": int(exp.timestamp()),
    }

    return json.dumps(payload)

def _sign_jwt(resource_url: str, target_sa: str | None = None) -> str:
    """Signs JWT payload using ADC and IAM credentials API.

    Uses Google Cloud's IAM Credentials API to sign a JWT. This requires the
    caller to have iap.webServiceVersions.accessViaIap permission on the
    target service account.
    SA must have the role "Service Account Token Creator" on itself.

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
    if target_sa is None:
        target_sa = source_credentials.service_account_email
    iam_client = iam_credentials_v1.IAMCredentialsClient(credentials=source_credentials)
    name = iam_client.service_account_path("-", target_sa)
    payload = _generate_jwt_payload(target_sa, resource_url)
    response = iam_client.sign_jwt(name=name, payload=payload)
    return response.signed_jwt

