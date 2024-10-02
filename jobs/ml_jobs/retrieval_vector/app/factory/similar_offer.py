from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np

from app.factory.handler import PredictionHandler
from app.factory.tops import SearchByTopsHandler
from app.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult
from app.retrieval.client import DefaultClient


class SimilarOfferHandler(PredictionHandler):
    """
    Handler for similar offer predictions.
    Supports both single and multiple items.
    """

    def handle(
        self,
        model: DefaultClient,
        request_data: PredictionRequest,
        fallback_client: Optional[PredictionHandler] = SearchByTopsHandler(),
    ) -> PredictionResult:
        """
        Handles the prediction request for similar offers based on item vectors.

        Args:
            model (DefaultClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.
            fallback_client (PredictionHandler): In case something gets wrong (items not found), fallback to this handler.

        Returns:
            PredictionResult: An object containing the predicted similar items.
        """
        logger.debug(
            "similar_offers",
            extra={
                "uuid": request_data.call_id,
                "items": request_data.items,
                "params": request_data.params,
                "size": request_data.size,
            },
        )
        if request_data.items is None or request_data.items == []:
            raise ValueError(
                "items or offer_id is required for similar_offer predictions."
            )

        # Specified items are excluded in similar offer predictions context
        excluded_items = request_data.items + request_data.excluded_items

        prediction_result = self._get_predictions_for_items(
            model, request_data, excluded_items=excluded_items
        )

        # If no predictions were found and fallback is activated
        if len(prediction_result.predictions) == 0 and fallback_client is not None:
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
                    excluded_items=excluded_items,
                ),
            )

        # If we have multiple predictions, compute the bests items by calculating the mean distance and selecting top N
        if len(request_data.items) > 1:
            return self._select_best_predictions(
                prediction_result.predictions,
                request_data.size,
            )
        else:
            return prediction_result

    def _get_predictions_for_items(
        self,
        model: DefaultClient,
        request_data: PredictionRequest,
        excluded_items: List[str] = [],
    ) -> PredictionResult:
        """
        Retrieves predictions for each item in the list.

        Args:
            model (DefaultClient): The model used to search for predictions.
            request_data (PredictionRequest): Request data containing search parameters.

        Returns:
            List[PredictionResult]: A list of predictions retrieved for the items.
        """
        prediction_result = PredictionResult(predictions=[])
        for item_id in request_data.items:
            logger.debug(f"Searching for item_id: {item_id}")
            vector = model.item_vector(item_id)
            if vector is not None:
                results = self.search_by_vector(
                    model=model,
                    vector=vector,
                    request_data=request_data,
                    excluded_items=excluded_items,
                )
                if len(results.predictions) > 0:
                    prediction_result.predictions.extend(results.predictions)
            else:
                logger.debug(f"No vector found for item_id: {item_id}")
        return prediction_result

    def _select_best_predictions(
        self, predictions: List[Dict], size: int
    ) -> PredictionResult:
        """
        Selects the best predictions by calculating the mean `_distance` value for each item,
        and then sorting and returning the top X predictions based on the smallest mean `_distance`.

        Args:
            predictions (List[Dict]): A list of predictions.
            size (int): The number of top results to return.
            items (List[str]): The input items to exclude from the predictions.

        Returns:
            List[Dict]: A list of predictions with the mean `_distance` for each item.
        """
        grouped_predictions = defaultdict(list)

        # Group predictions by item_id
        for entry in predictions:
            grouped_predictions[entry["item_id"]].append(entry)

        averaged_predictions = []
        # Calculate mean distance over multiple predictions for the same item
        for _, entries in grouped_predictions.items():
            mean_distance = np.mean([entry["_distance"] for entry in entries])
            best_entry = entries[0].copy()
            best_entry["_distance"] = mean_distance
            averaged_predictions.append(best_entry)

        averaged_predictions.sort(key=lambda x: x["_distance"])
        return PredictionResult(predictions=averaged_predictions[:size])
