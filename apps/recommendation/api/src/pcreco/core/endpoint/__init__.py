from abc import ABC


class AbstractEndpoint(ABC):
    def __init__(self, endpoint_name: str, size, fallback_endpoints=[]) -> None:
        """
        endpoint_name : Default endpoint
        fallback_endpoints : List of endpoints to retry in case no results or timeout error
        """
        self.endpoint_name = str(endpoint_name.value)
        self.size = size
        self.fallback_endpoints = [str(x.value) for x in fallback_endpoints]
        self.model_version = None
        self.model_display_name = None
