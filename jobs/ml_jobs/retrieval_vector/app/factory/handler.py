from abc import ABC, abstractmethod
from typing import Dict

from app.models import PredictionRequest
from app.retrieval.client import DefaultClient


class PredictionHandler(ABC):
    """
    Abstract base class for prediction handlers.
    Each handler must implement the handle method.
    """

    @abstractmethod
    def handle(self, model: DefaultClient, request_data: PredictionRequest) -> Dict:
        pass
