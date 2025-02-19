from app.factory.handler import PredictionHandler
from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult
from app.retrieval.text_client import TextClient


class SemanticHandler(PredictionHandler):
    """
    Handler for semantic predictions.
    """

    def handle(
        self, model: TextClient, request_data: PredictionRequest
    ) -> PredictionResult:
        """
        Handles the prediction request for semantic text based on semantic vectors.

        Args:
            model (TextClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.

        Returns:
            PredictionResult: An object containing the predicted similar items.
        """
        if request_data.text is None:
            raise ValueError("Text is required for semantic predictions.")

        if request_data.re_rank:
            raise ValueError("Re-rank is not supported for semantic predictions.")

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
        return self.search_by_vector(
            model=model,
            vector=vector,
            request_data=request_data,
            excluded_items=request_data.excluded_items,
        )
