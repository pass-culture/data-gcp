from pcreco.utils.env_vars import (
    DEFAULT_RECO_MODEL,
    DEFAULT_SIMILAR_OFFER_MODEL,
)
import pcreco.core.model_selection.recommendation as recommendation_endpoints
import pcreco.core.model_selection.similar_offer as similar_offer_endpoints


def select_reco_model_params(
    model_endpoint: str,
) -> recommendation_endpoints.RecommendationDefaultModel:
    """Choose the model to apply Recommendation"""
    if model_endpoint not in list(
        recommendation_endpoints.RECOMMENDATION_ENDPOINTS.keys()
    ):
        model_endpoint = DEFAULT_RECO_MODEL
    return recommendation_endpoints.RECOMMENDATION_ENDPOINTS[model_endpoint]


def select_sim_model_params(
    model_endpoint: str,
) -> similar_offer_endpoints.SimilarOfferDefaultModel:
    """Choose the model to apply for Similar Offers"""
    if model_endpoint not in list(
        similar_offer_endpoints.SIMILAR_OFFER_ENDPOINTS.keys()
    ):
        model_endpoint = DEFAULT_SIMILAR_OFFER_MODEL
    return similar_offer_endpoints.SIMILAR_OFFER_ENDPOINTS[model_endpoint]
