from typing import Optional

from app.factory.diversification import DiversificationPipeline
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

    def apply_semantic_sampling(self, scored_offers, output_size, use_qi):
        valid_offers = [
            item for item in scored_offers if item["semantic_embedding"] is not None
        ]
        sampled_offer_ids = DiversificationPipeline(
            item_semantic_embeddings=[
                item["semantic_embedding"] for item in valid_offers
            ],
            ids=[item["item_id"] for item in valid_offers],
            scores=[1 - float(item["_distance"]) for item in valid_offers],
        ).get_sampled_ids(K_DPP=output_size, use_qi=use_qi)
        sampled_offer_ids_set = set(sampled_offer_ids)
        # Filter scored_offers to get recommendable_offers_diverisified
        recommendable_offers_diverisified = [
            row for row in valid_offers if row["item_id"] in sampled_offer_ids_set
        ]

        return recommendable_offers_diverisified

    def remove_semantic_embedding_from_results(self, results):
        clean_results = []
        for res in results:
            clean_results.append(
                {k: v for k, v in res.items() if k != "semantic_embedding"}
            )
        return clean_results

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
        pre_dpp_size = request_data.pre_dpp_size
        output_size = request_data.size

        logger.debug(
            "recommendation",
            extra={
                "uuid": request_data.call_id,
                "user_id": request_data.user_id,
                "params": request_data.params,
                "pre_dpp_size": pre_dpp_size,
                "output_size": output_size,
            },
        )
        if request_data.user_id is None:
            raise ValueError("user_id is required for recommendation predictions.")

        results = PredictionResult(predictions=[])
        vector = model.user_vector(request_data.user_id)

        if vector is not None:
            request_data.size = pre_dpp_size
            results_raw = self.search_by_vector(
                model=model,
                vector=vector,
                request_data=request_data,
            )
            dpp_predictions = self.remove_semantic_embedding_from_results(
                self.apply_semantic_sampling(
                    scored_offers=results_raw.predictions,
                    output_size=output_size,
                    use_qi=request_data.use_qi,
                )
            )
            results.predictions = dpp_predictions

        # If no predictions are found and fallback is active
        if len(results.predictions) == 0 and fallback_client is not None:
            results = fallback_client.handle(
                model,
                request_data=PredictionRequest(
                    model_type="tops",
                    size=output_size,
                    debug=request_data.debug,
                    prefilter=request_data.is_prefilter,
                    re_rank=request_data.re_rank,
                    vector_column_name="booking_number_desc",
                    similarity_metric="dot",
                    params=request_data.params,
                    call_id=request_data.call_id,
                    user_id=request_data.user_id,
                    items=request_data.items,
                ),
            )
            cleaned_predictions = self.remove_semantic_embedding_from_results(
                results.predictions
            )
            results.predictions = cleaned_predictions

        return results
