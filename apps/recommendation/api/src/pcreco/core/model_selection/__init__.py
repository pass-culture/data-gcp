from pcreco.utils.env_vars import (
    DEFAULT_RECO_MODEL,
    DEFAULT_SIMILAR_OFFER_MODEL,
)
import pcreco.core.model_selection.recommendation as recommendation_endpoints
import pcreco.core.model_selection.similar_offer as similar_offer_endpoints
from pcreco.core.model_selection.model_configuration import (
    ModelConfiguration,
    ModelFork,
)
from pcreco.core.user import User
from pcreco.core.offer import Offer
from loguru import logger


RECOMMENDATION_ENDPOINTS = {
    # Default endpoint
    "default": ModelFork(
        warm_start_model=recommendation_endpoints.retrieval_reco,
        cold_start_model=recommendation_endpoints.retrieval_filter,
        bookings_count=2,
        clicks_count=25,
        favorites_count=None,
    ),
    "version_b": ModelFork(
        warm_start_model=recommendation_endpoints.retrieval_reco_version_b,
        cold_start_model=recommendation_endpoints.retrieval_filter_version_b,
        bookings_count=0,
    ),
    "top_offers": ModelFork(
        warm_start_model=recommendation_endpoints.retrieval_filter,
        cold_start_model=recommendation_endpoints.retrieval_filter,
        bookings_count=None,
        clicks_count=None,
        favorites_count=None,
    ),
    # Force model default enpoint
    "default_algo": ModelFork(
        warm_start_model=recommendation_endpoints.retrieval_reco,
        cold_start_model=recommendation_endpoints.retrieval_reco,
        bookings_count=0,
        clicks_count=0,
        favorites_count=0,
    ),
    # Force cold start model based on top offers
    "cold_start": ModelFork(
        warm_start_model=recommendation_endpoints.retrieval_filter,
        cold_start_model=recommendation_endpoints.retrieval_filter,
        bookings_count=None,
        clicks_count=None,
        favorites_count=None,
    ),
}

SIMILAR_OFFER_ENDPOINTS = {
    # Default version a
    "default": ModelFork(
        warm_start_model=similar_offer_endpoints.retrieval_offer,
        cold_start_model=similar_offer_endpoints.retrieval_offer,
        bookings_count=0,
    ),
    "version_b": ModelFork(
        warm_start_model=similar_offer_endpoints.retrieval_offer_version_b,
        cold_start_model=similar_offer_endpoints.retrieval_cs_offer,
        bookings_count=0,
    ),
    # Force cold start mode
    "cold_start": ModelFork(
        warm_start_model=similar_offer_endpoints.retrieval_cs_offer,
        cold_start_model=similar_offer_endpoints.retrieval_cs_offer,
        bookings_count=None,
        clicks_count=None,
        favorites_count=None,
    ),
}


def select_reco_model_params(model_endpoint: str, user: User) -> ModelConfiguration:
    """Choose the model to apply Recommendation based on user interaction"""
    if model_endpoint not in list(RECOMMENDATION_ENDPOINTS.keys()):
        model_endpoint = DEFAULT_RECO_MODEL
    logger.info(f"{user.id}: reco_endpoint {model_endpoint}")
    model_fork = RECOMMENDATION_ENDPOINTS[model_endpoint]
    return model_fork.get_user_status(user=user)


def select_sim_model_params(model_endpoint: str, offer: Offer) -> ModelConfiguration:
    """Choose the model to apply for Similar Offers based on offer interaction"""
    if model_endpoint not in list(SIMILAR_OFFER_ENDPOINTS.keys()):
        model_endpoint = DEFAULT_SIMILAR_OFFER_MODEL
    logger.info(f"{offer.id}: sim_offer_endpoint {model_endpoint}")
    model_fork = SIMILAR_OFFER_ENDPOINTS[model_endpoint]
    return model_fork.get_offer_status(offer=offer)
