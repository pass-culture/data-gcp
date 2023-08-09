from abc import ABC


class AbstractEndpoint(ABC):
    def __init__(self, endpoint_name) -> None:
        self.endpoint_name = endpoint_name
        self.model_version = None
        self.model_display_name = None
