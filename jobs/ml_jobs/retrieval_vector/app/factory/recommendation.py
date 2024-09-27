from typing import Dict, Optional

from app.factory.handler import PredictionHandler
from app.factory.tops import SearchByTopsHandler
from app.logger import logger
from app.models import PredictionRequest
from app.retrieval.client import DefaultClient
from app.services import search_by_vector


class RecommendationHandler(PredictionHandler):
    """
    Handler for recommendation predictions.
    """

    def handle(
        self,
        model: DefaultClient,
        request_data: PredictionRequest,
        fallback_client: Optional[PredictionHandler] = SearchByTopsHandler(),
    ) -> Dict:
        """
        Handles the prediction request for user recommendation.

        Args:
            model (DefaultClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.
            fallback_client (PredictionHandler): In case something gets wrong (user not found), fallback to this handler.

        Returns:
            Dict: A dictionary containing the predicted items.
        """
        results = {"predictions": []}
        vector = model.user_vector(request_data.user_id)
        logger.debug(
            "recommendation",
            extra={
                "uuid": request_data.call_id,
                "user_id": request_data.user_id,
                "params": request_data.params,
                "size": request_data.size,
            },
        )
        if vector is not None:
            results = search_by_vector(
                model=model,
                vector=vector,
                size=request_data.size,
                selected_params=request_data.params,
                debug=request_data.debug,
                call_id=request_data.call_id,
                prefilter=request_data.is_prefilter,
                vector_column_name=request_data.vector_column_name,
                similarity_metric=request_data.similarity_metric,
                re_rank=request_data.re_rank,
                user_id=request_data.user_id,
            )

        # If no predictions are found and fallback
        if len(results["predictions"]) == 0 and fallback_client is not None:
            return fallback_client.handle(
                model,
                PredictionRequest(
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
