from google.auth.exceptions import DefaultCredentialsError
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from pcreco.utils.env_vars import MODEL_END_POINT
import socket

socket.setdefaulttimeout(300)


class AIPlatformService:
    def get(self, default=None):
        try:
            client = ClientOptions(api_endpoint=MODEL_END_POINT)
            service = discovery.build("ml", "v1", client_options=client, cache=False)
        except DefaultCredentialsError:
            return default
        return service


ai_platform_service = AIPlatformService()
