from dataclasses import dataclass
from pcreco.utils.env_vars import (
    ENV_SHORT_NAME,
    RECOMMENDABLE_OFFER_LIMIT,
    COLD_START_RECOMMENDABLE_OFFER_LIMIT,
)


@dataclass
class RecommendationDefaultModel:
    name: str
    force_cold_start: bool = False
    force_model: bool = False
    endpoint_name: str = f"recommendation_default_{ENV_SHORT_NAME}"
    retrieval_order_query: str = "is_geolocated DESC, booking_number DESC"
    offer_limit: int = RECOMMENDABLE_OFFER_LIMIT


@dataclass
class RecommendationVersionBModel(RecommendationDefaultModel):
    endpoint_name: str = f"recommendation_version_b_{ENV_SHORT_NAME}"


@dataclass
class RecommendationColdStartVersionB(RecommendationDefaultModel):
    cold_start_model_endpoint_name: str = (
        f"recommendation_cold_start_model_{ENV_SHORT_NAME}"
    )


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
