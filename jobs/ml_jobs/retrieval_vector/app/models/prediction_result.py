from dataclasses import dataclass
from typing import List, Optional


@dataclass
class PredictionResult:
    """
    Pydantic output model for the prediction model.
    """

    predictions: Optional[List[dict]]
