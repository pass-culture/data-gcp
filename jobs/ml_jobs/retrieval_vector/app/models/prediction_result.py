from dataclasses import dataclass
from typing import List


@dataclass
class SearchType:
    """
    Represents the different types of search operations or results.
    This class defines string constants that can be used to identify
    the nature of a search operation or the type of result obtained.
    Attributes:
        VECTOR (str): Represents a search based on vector similarity.
        TOPS (str): Represents a search for top-ranking items, possibly based on popularity or other metrics.
        ERROR (str): Indicates that an error occurred during the search.
        AGGREGATED_VECTORS (str): Represents a search based on aggregated vectors.
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
    """

    predictions: List[dict]
