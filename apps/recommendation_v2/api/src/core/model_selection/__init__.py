from utils.env_vars import (
    # DEFAULT_RECO_MODEL,
    DEFAULT_SIMILAR_OFFER_MODEL,
)

# import core.model_selection.recommendation as recommendation_endpoints
import core.model_selection.similar_offer as similar_offer_endpoints
from core.model_selection.model_configuration import (
    ModelConfiguration,
    ModelFork,
)
from schemas.user import User
from schemas.offer import Offer
from loguru import logger


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


def select_sim_model_params(model_endpoint: str, offer: Offer) -> ModelConfiguration:
    """Choose the model to apply for Similar Offers based on offer interaction"""
    if model_endpoint not in list(SIMILAR_OFFER_ENDPOINTS.keys()):
        model_endpoint = DEFAULT_SIMILAR_OFFER_MODEL
    logger.info(f"{offer.offer_id}: sim_offer_endpoint {model_endpoint}")
    model_fork = SIMILAR_OFFER_ENDPOINTS[model_endpoint]
    return model_fork.get_offer_status(offer=offer)
