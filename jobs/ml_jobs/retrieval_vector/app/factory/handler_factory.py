from app.factory.handler import PredictionHandler
from app.factory.recommendation import RecommendationHandler
from app.factory.semantic import SemanticHandler
from app.factory.similar_offer import SimilarOfferHandler
from app.factory.tops import SearchByTopsHandler
from app.retrieval.constants import EmbeddingModelTypes


class PredictionHandlerFactory:
    """
    Factory for creating the appropriate handler based on model_type.
    """

    @staticmethod
    def get_handler(
        request_type: str, embedding_model_type: EmbeddingModelTypes
    ) -> PredictionHandler:
        if request_type == "recommendation":
            if embedding_model_type != EmbeddingModelTypes.TWO_TOWER:
                raise ValueError(
                    f"Request type '{request_type}' is only supported for {EmbeddingModelTypes.TWO_TOWER} models. Currently using '{embedding_model_type}' model."
                )
            return RecommendationHandler()

        elif request_type == "semantic":
            if embedding_model_type != EmbeddingModelTypes.SEMANTIC:
                raise ValueError(
                    f"Request type '{request_type}' is only supported for {EmbeddingModelTypes.SEMANTIC} models. Currently using '{embedding_model_type}' model."
                )
            return SemanticHandler()

        elif request_type == "similar_offer":
            return SimilarOfferHandler()

        elif request_type in ("filter", "tops"):
            return SearchByTopsHandler()
        else:
            raise ValueError(f"Unknown request_type: {request_type}")
