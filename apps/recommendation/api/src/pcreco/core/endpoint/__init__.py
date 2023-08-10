from abc import ABC


class AbstractEndpoint(ABC):
    def __init__(self, endpoint_name, fallback_endpoints=[]) -> None:
        """
        endpoint_name : Default endpoint
        fallback_endpoints : List of endpoints to retry in case no results or timeout error
        """
        self.endpoint_name = endpoint_name
        self.fallback_endpoints = fallback_endpoints
        self.model_version = None
        self.model_display_name = None
