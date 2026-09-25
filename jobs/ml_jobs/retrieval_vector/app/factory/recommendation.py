from app.factory.handler import PredictionHandler
from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult
from app.retrieval.reco_client import RecoClient


class RecommendationHandler(PredictionHandler):
    """
    Handler for recommendation predictions.
    """

    def handle(
        self,
        model: RecoClient,
        request_data: PredictionRequest,
    ) -> PredictionResult:
        """
        Handles the prediction request for user recommendation.

        Args:
            model (RecoClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.

        Returns:
            PredictionResult: An object containing the predicted items and the model type.
        """
        logger.info(
            "recommendation",
            extra={
                "uuid": request_data.call_id,
                "user_id": request_data.user_id,
                "params": request_data.params,
                "size": request_data.size,
            },
        )
        if request_data.user_id is None:
            raise ValueError("user_id is required for recommendation predictions.")

        vector = model.user_vector(request_data.user_id)
        if vector is not None:
            results = self.search_by_vector(
                model=model,
                vector=vector,
                request_data=request_data,
            )

            if len(results.predictions) > 0:
                return results

        logger.info(
            "No recommendations found, returning empty list",
            extra={"uuid": request_data.call_id, "user_id": request_data.user_id},
        )
        return PredictionResult(predictions=[])
