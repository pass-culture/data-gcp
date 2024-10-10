from app.factory.handler import PredictionHandler
from app.factory.recommendation import RecommendationHandler
from app.factory.semantic import SemanticHandler
from app.factory.similar_offer import SimilarOfferHandler
from app.factory.tops import SearchByTopsHandler


class PredictionHandlerFactory:
    """
    Factory for creating the appropriate handler based on model_type.
    """

    @staticmethod
    def get_handler(model_type: str) -> PredictionHandler:
        if model_type == "recommendation":
            return RecommendationHandler()
        elif model_type == "semantic":
            return SemanticHandler()
        elif model_type == "similar_offer":
            return SimilarOfferHandler()
        elif model_type in ("filter", "tops"):
            return SearchByTopsHandler()
        else:
            raise ValueError(f"Unknown model_type: {model_type}")
