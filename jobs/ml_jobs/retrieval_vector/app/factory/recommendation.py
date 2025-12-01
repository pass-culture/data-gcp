from app.factory.handler import PredictionHandler
from app.factory.tops import SearchByTopsHandler
from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult
from app.retrieval.reco_client import RecoClient


class RecommendationHandler(PredictionHandler):
    """
    Handler for recommendation predictions.
    """

    fallback_client = SearchByTopsHandler()

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
        logger.debug(
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

        logger.debug(
            "No recommendations found, attempting fallback.",
            extra={"uuid": request_data.call_id, "user_id": request_data.user_id},
        )
        return self.fallback_client.handle(
            model,
            request_data=PredictionRequest(
                model_type="tops",
                size=request_data.size,
                debug=request_data.debug,
                prefilter=request_data.is_prefilter,
                re_rank=request_data.re_rank,
                vector_column_name="booking_number_desc",
                params=request_data.params,
                call_id=request_data.call_id,
                user_id=request_data.user_id,
                items=request_data.items,
            ),
        )
