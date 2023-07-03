from pcreco.utils.env_vars import (
    DEFAULT_RECO_MODEL,
    DEFAULT_SIMILAR_OFFER_MODEL,
)
import pcreco.core.model_selection.recommendation as recommendation_endpoints
import pcreco.core.model_selection.similar_offer as similar_offer_endpoints
from pcreco.core.model_selection.model_configuration import ModelConfiguration
from pcreco.core.user import User
from loguru import logger


def select_reco_model_params(model_endpoint: str, user: User) -> ModelConfiguration:
    """Choose the model to apply Recommendation"""
    if model_endpoint not in list(
        recommendation_endpoints.RECOMMENDATION_ENDPOINTS.keys()
    ):
        model_endpoint = DEFAULT_RECO_MODEL
    logger.info(f"{user.id}: reco_endpoint {model_endpoint}")
    return recommendation_endpoints.RECOMMENDATION_ENDPOINTS[model_endpoint].get_scorer(
        user
    )


def select_sim_model_params(
    model_endpoint: str,
) -> ModelConfiguration:
    """Choose the model to apply for Similar Offers"""
    if model_endpoint not in list(
        similar_offer_endpoints.SIMILAR_OFFER_ENDPOINTS.keys()
    ):
        model_endpoint = DEFAULT_SIMILAR_OFFER_MODEL
    return similar_offer_endpoints.SIMILAR_OFFER_ENDPOINTS[model_endpoint]
