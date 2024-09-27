from typing import Dict

from app.factory.handler import PredictionHandler
from app.logger import logger
from app.models import PredictionRequest
from app.retrieval.client import DefaultClient
from app.services import search_by_vector


class SemanticHandler(PredictionHandler):
    """
    Handler for semantic predictions.
    """

    def handle(self, model: DefaultClient, request_data: PredictionRequest) -> Dict:
        """
        Handles the prediction request for semantic text based on semantic vectors.

        Args:
            model (DefaultClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.

        Returns:
            Dict: A dictionary containing the predicted similar items.
        """
        if request_data.text is None:
            raise ValueError("Text is required for semantic predictions.")

        vector = model.text_vector(request_data.text)
        logger.debug(
            "semantic",
            extra={
                "uuid": request_data.call_id,
                "text": request_data.text,
                "params": request_data.params,
                "size": request_data.size,
            },
        )
        return search_by_vector(
            model=model,
            vector=vector,
            size=request_data.size,
            selected_params=request_data.params,
            debug=request_data.debug,
            call_id=request_data.call_id,
            prefilter=request_data.is_prefilter,
            vector_column_name=request_data.vector_column_name,
            similarity_metric=request_data.similarity_metric,
            re_rank=False,  # cannot re-rank semantic predictions for now
        )
