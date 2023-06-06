from dataclasses import dataclass
from pcreco.utils.env_vars import (
    ENV_SHORT_NAME,
    DEFAULT_RECO_MODEL,
    DEFAULT_SIMILAR_OFFER_MODEL,
    RECOMMENDABLE_OFFER_LIMIT,
    COLD_START_RECOMMENDABLE_OFFER_LIMIT,
)


@dataclass
class RecommendationDefaultModel:
    name: str
    force_cold_start: bool = False
    force_model: bool = False
    endpoint_name: str = f"recommendation_default_{ENV_SHORT_NAME}"
    order_query: str = "is_geolocated DESC, booking_number DESC"
    offer_limit: int = RECOMMENDABLE_OFFER_LIMIT


@dataclass
class RecommendationVersionBModel(RecommendationDefaultModel):
    endpoint_name: str = f"recommendation_version_b_{ENV_SHORT_NAME}"


@dataclass
class RecommendationColdStartVersionB(RecommendationDefaultModel):
    cold_start_model_endpoint_name: str = (
        f"recommendation_cold_start_model_{ENV_SHORT_NAME}"
    )


@dataclass
class SimilarOfferDefaultModel:
    name: str
    endpoint_name: str = f"similar_offers_default_{ENV_SHORT_NAME}"
    order_query = "booking_number DESC"


RECOMMENDATION_ENDPOINTS = {
    "default": RecommendationDefaultModel("default"),
    "algo_default": RecommendationDefaultModel("algo_default", force_model=True),
    "cold_start": RecommendationDefaultModel("cold_start", force_cold_start=True),
    "version_b": RecommendationVersionBModel("version_b"),
    "algo_version_b": RecommendationVersionBModel("algo_version_b", force_model=True),
    "cold_start_b": RecommendationColdStartVersionB(
        "cold_start_b", offer_limit=COLD_START_RECOMMENDABLE_OFFER_LIMIT
    ),
}

SIMILAR_OFFER_ENDPOINTS = {"default": SimilarOfferDefaultModel("default")}


def select_reco_model_params(model_endpoint: str) -> RecommendationDefaultModel:
    """Choose the model to apply Recommendation"""
    if model_endpoint not in list(RECOMMENDATION_ENDPOINTS.keys()):
        model_endpoint = DEFAULT_RECO_MODEL
    return RECOMMENDATION_ENDPOINTS[model_endpoint]


def select_sim_model_params(model_endpoint: str) -> SimilarOfferDefaultModel:
    """Choose the model to apply for Similar Offers"""
    if model_endpoint not in list(SIMILAR_OFFER_ENDPOINTS.keys()):
        model_endpoint = DEFAULT_SIMILAR_OFFER_MODEL
    return SIMILAR_OFFER_ENDPOINTS[model_endpoint]
