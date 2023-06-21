from abc import ABC, abstractmethod


class ModelEndpoint(ABC):
    @abstractmethod
    def model_score(self, item_input, size):
        pass
