from abc import ABC, abstractmethod


class ModelEndpoint(ABC):
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name
        self.model_version = None
        self.model_display_name = None

    @abstractmethod
    def model_score(self, size):
        pass
