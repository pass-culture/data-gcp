from dataclasses import dataclass
from typing import List


@dataclass
class SearchType:
    """
    Enum-like class for search types.
    Attributes:
        VECTOR (str): Represents vector-based search.
        TOPS (str): Represents tops-based search.
    """

    VECTOR = "vector"
    TOPS = "tops"
    ERROR = "error"
    AGGREGATED_VECTORS = "aggregated_vectors"


@dataclass
class PredictionResult:
    """
    Class representing the result of a prediction request.
    Attributes:
        predictions (List[dict]): List of predicted items.
        search_type (str): Type of search performed (e.g., "vector", "tops").
    """

    predictions: List[dict]
    search_type: SearchType
