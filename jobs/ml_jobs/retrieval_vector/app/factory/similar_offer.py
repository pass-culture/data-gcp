from collections import defaultdict
from typing import Dict, List

import numpy as np

from app.factory.handler import PredictionHandler
from app.factory.tops import SearchByTopsHandler
from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult, SearchType
from app.retrieval.client import DefaultClient
from app.retrieval.constants import DISTANCE_COLUMN_NAME, SEARCH_TYPE_COLUMN_NAME


class SimilarOfferHandler(PredictionHandler):
    """
    Handler for similar offer predictions.
    Supports both single and multiple items.
    """

    fallback_client: PredictionHandler = SearchByTopsHandler()

    def handle(
        self,
        model: DefaultClient,
        request_data: PredictionRequest,
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
        if len(prediction_result.predictions) > 0:
            return prediction_result

        # If no predictions were found
        return self.fallback_client.handle(
            model,
            request_data=PredictionRequest(
                model_type="tops",
                size=request_data.size,
                debug=request_data.debug,
                prefilter=request_data.is_prefilter,
                re_rank=request_data.re_rank,
                vector_column_name="booking_number_desc",
                similarity_metric="dot",
                params=request_data.params,
                call_id=request_data.call_id,
                user_id=request_data.user_id,
                items=request_data.items,
                excluded_items=excluded_items,
            ),
        )

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

        # Iterate over each item in the request_data.items and search for predictions
        predictions_list = []
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
                    predictions_list += results.predictions
            else:
                logger.debug(f"No vector found for item_id: {item_id}")

        # If we have multiple predictions, compute the bests items by calculating the mean distance and selecting top N
        if len(request_data.items) > 1:
            return PredictionResult(
                predictions=self._select_best_predictions(
                    predictions_list,
                    request_data.size,
                )
            )

        return PredictionResult(predictions=predictions_list)

    def _select_best_predictions(
        self, predictions: List[Dict], size: int
    ) -> List[Dict]:
        """
        Selects the best predictions by calculating the mean `_distance` value for each item,
        and then sorting and returning the top X predictions based on the smallest mean `_distance`.

        Args:
            predictions (List[Dict]): A list of predictions.
            size (int): The number of top results to return.

        Returns:
            List[Dict]: A list of predictions with the mean `_distance` for each item.
        """
        grouped_predictions = defaultdict(list)

        # Group predictions by item_id
        for entry in predictions:
            grouped_predictions[entry["item_id"]].append(entry)

        # Calculate mean distance over multiple predictions for the same item
        averaged_predictions = []
        for _, entries in grouped_predictions.items():
            mean_distance = np.mean([entry[DISTANCE_COLUMN_NAME] for entry in entries])
            best_entry = entries[0].copy()
            best_entry[DISTANCE_COLUMN_NAME] = mean_distance
            best_entry[SEARCH_TYPE_COLUMN_NAME] = SearchType.AGGREGATED_VECTORS
            averaged_predictions.append(best_entry)

        averaged_predictions.sort(key=lambda x: x[DISTANCE_COLUMN_NAME])

        return averaged_predictions[:size]
