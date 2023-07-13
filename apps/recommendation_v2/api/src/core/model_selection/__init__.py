from utils.env_vars import (
    DEFAULT_SIMILAR_OFFER_MODEL,
)

import core.model_selection.similar_offer as similar_offer_endpoints
from core.model_selection.model_configuration import ModelConfiguration
from schemas.user import User

# from loguru import logger


def select_sim_model_params(
    model_endpoint: str,
) -> ModelConfiguration:
    """Choose the model to apply for Similar Offers"""
    if model_endpoint not in list(
        similar_offer_endpoints.SIMILAR_OFFER_ENDPOINTS.keys()
    ):
        model_endpoint = DEFAULT_SIMILAR_OFFER_MODEL
    return similar_offer_endpoints.SIMILAR_OFFER_ENDPOINTS[model_endpoint]
