from app.factory.handler import PredictionHandler
from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult
from app.retrieval.client import DefaultClient


class SearchByTopsHandler(PredictionHandler):
    """
    Handler for filter predictions.
    """

    def handle(
        self, model: DefaultClient, request_data: PredictionRequest
    ) -> PredictionResult:
        """
        Handles the prediction request for tops offers based.

        Args:
            model (DefaultClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.

        Returns:
            PredictionResult: An object containing the predicted items.
        """
        logger.debug(
            "filter",
            extra={
                "uuid": request_data.call_id,
                "params": request_data.params,
                "size": request_data.size,
            },
        )
        return self.search_by_tops(model=model, request_data=request_data)
