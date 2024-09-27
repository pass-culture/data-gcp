from collections import defaultdict
from typing import Dict, List, Optional

from docarray import Document

from app.factory.handler import PredictionHandler
from app.factory.tops import SearchByTopsHandler
from app.logger import logger
from app.models import PredictionRequest
from app.retrieval.client import DefaultClient
from app.services import search_by_vector


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
    ) -> Dict:
        """
        Handles the prediction request for similar offers based on item vectors.

        Args:
            model (DefaultClient): The model that performs the search.
            request_data (PredictionRequest): The request data containing parameters and item IDs.
            fallback_client (PredictionHandler): In case something gets wrong (items not found), fallback to this handler.

        Returns:
            Dict: A dictionary containing the predicted similar items.
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

        predictions = self._get_predictions_for_items(model, request_data)

        # If no predictions are found and fallback
        if len(predictions) == 0 and fallback_client is not None:
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

        # Compute the best items by calculating the mean distance and selecting the expected top X
        if len(request_data.items) > 0:
            predictions = self._select_best_predictions(
                predictions, request_data.size, request_data.items
            )
        return {"predictions": predictions}

    def _get_predictions_for_items(
        self, model: DefaultClient, request_data: PredictionRequest
    ) -> List[Dict]:
        """
        Retrieves predictions for each item in the list.

        Args:
            model (DefaultClient): The model used to search for predictions.
            request_data (PredictionRequest): Request data containing search parameters.

        Returns:
            List[Dict]: A list of predictions retrieved for the items.
        """
        predictions = []
        for item_id in request_data.items:
            vector = model.offer_vector(item_id)
            if vector is not None:
                prediction = self._search_by_vector(
                    model, request_data, vector, item_id
                )
                if prediction:
                    predictions.extend(prediction["predictions"])
            else:
                logger.debug(f"No vector found for item_id: {item_id}")
        return predictions

    def _search_by_vector(
        self,
        model: DefaultClient,
        request_data: PredictionRequest,
        vector: Document,
        item_id: str,
    ) -> Dict:
        """
        Performs the search using the vector for a given item.

        Args:
            model (DefaultClient): The model used for searching.
            request_data (PredictionRequest): Request data containing search parameters.
            vector (Document): The vector representation of the item.
            item_id (str): The item ID for which the search is performed.

        Returns:
            Dict: A dictionary containing the search results.
        """
        return search_by_vector(
            model=model,
            vector=vector,
            size=request_data.size
            + 1,  # Add 1 to the size to account for the item that will be removed
            selected_params=request_data.params,
            debug=request_data.debug,
            call_id=request_data.call_id,
            prefilter=request_data.is_prefilter,
            vector_column_name=request_data.vector_column_name,
            similarity_metric=request_data.similarity_metric,
            re_rank=request_data.re_rank,
            item_id=item_id,
            user_id=request_data.user_id,
        )

    def _select_best_predictions(
        self, predictions: List[Dict], size: int, items: List[str]
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
            # Skip the item that was used for the search
            if entry["item_id"] not in items:
                grouped_predictions[entry["item_id"]].append(entry)

        # Calculate the mean _distance for each item and store results
        averaged_predictions = []
        for item_id, entries in grouped_predictions.items():
            mean_distance = self._calculate_mean_distance(entries)
            # Use the first entry as a template, but update the distance to the mean distance
            best_entry = entries[0].copy()
            best_entry["_distance"] = mean_distance
            averaged_predictions.append(best_entry)

        # Sort the predictions by mean _distance in ascending order and return top X
        averaged_predictions.sort(key=lambda x: x["_distance"])
        return averaged_predictions[:size]  # Return top 'size' predictions

    def _calculate_mean_distance(self, entries: List[Dict]) -> float:
        """
        Calculates the mean `_distance` for a list of predictions.

        Args:
            entries (List[Dict]): A list of prediction entries for the same item.

        Returns:
            float: The mean `_distance` value for the predictions.
        """
        total_distance = sum(entry["_distance"] for entry in entries)
        return total_distance / len(entries)
