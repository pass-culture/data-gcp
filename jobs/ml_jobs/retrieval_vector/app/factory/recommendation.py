from typing import Optional

from app.factory.handler import PredictionHandler
from app.factory.tops import SearchByTopsHandler
from app.logger import logger
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
        fallback_client: Optional[PredictionHandler] = SearchByTopsHandler(),
    ) -> PredictionResult:
        """
        Handles the prediction request for user recommendation.

        Args:
            model (RecoClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.
            fallback_client (PredictionHandler): In case something gets wrong (user not found), fallback to this handler.

        Returns:
            DictPredictionResult: An object containing the recommended predicted items.
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

        results = PredictionResult(predictions=[])
        vector = model.user_vector(request_data.user_id)

        if vector is not None:
            results = self.search_by_vector(
                model=model,
                vector=vector,
                request_data=request_data,
            )

        # If no predictions are found and fallback is active
        if len(results.predictions) == 0 and fallback_client is not None:
            return fallback_client.handle(
                model,
                request_data=PredictionRequest(
                    model_type="filter",
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
        else:
            return results
