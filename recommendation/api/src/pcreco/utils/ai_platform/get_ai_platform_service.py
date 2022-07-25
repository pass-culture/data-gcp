from google.auth.exceptions import DefaultCredentialsError
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery


def get_ai_platform_service(MODEL_END_POINT, default=None):
    try:
        client = ClientOptions(api_endpoint=MODEL_END_POINT)
        ai_platform_service = discovery.build(
            "ml",
            "v1",
            client_options=client,
            cache_discovery=False,
        )
        return ai_platform_service
    except DefaultCredentialsError:
        return default
