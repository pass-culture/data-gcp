from dataclasses import dataclass
from pcreco.utils.env_vars import (
    ENV_SHORT_NAME,
)
from pcreco.core.user import User
import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer.recommendation import (
    RecommendationEndpoint,
    QPIEndpoint,
    DummyEndpoint,
)
from pcreco.core.model_selection.model_configuration import ModelConfiguration


@dataclass
class InteractionRules:
    bookings_count: int = 2
    clicks_count: int = 25
    favorites_count: int = None

    def get_model_status(self, user: User):
        if self.favorites_count is not None:
            if user.favorites_count >= self.favorites_count:
                return True
        if self.bookings_count is not None:
            if user.bookings_count >= self.bookings_count:
                return True
        if self.clicks_count is not None:
            if user.clicks_count >= self.clicks_count:
                return True
        return False


@dataclass
class DefaultModel:
    model_name: str
    cold_start_name: str
    cold_start_rules: InteractionRules

    def get_scorer(self, user: User):
        """Defines which model (cold_start or model) to use depending of the context"""
        model_type = self.cold_start_rules.get_model_status(user=user)
        if model_type:
            return MODEL_ENDPOINTS[self.model_name], "model"
        else:
            return MODEL_ENDPOINTS[self.cold_start_name], "cold_start"


MODEL_ENDPOINTS = {
    "default": ModelConfiguration(
        name="default",
        description="""
        Default model:
        Takes the top {retrieval_limit} offers nearest the user iris (SQL)
        Takes the top {ranking_limit} recommendable (default endpoint) (NN)
        Applies diversification filter
        """,
        scorer=offer_scorer.OfferIrisRetrieval,
        scorer_order_columns="score",
        scorer_order_ascending=False,
        endpoint=RecommendationEndpoint(f"recommendation_default_{ENV_SHORT_NAME}"),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=10_000,
        ranking_order_query="is_geolocated DESC, booking_number DESC",
        ranking_limit=50,
        default_shuffle_recommendation=True,
        default_mixing_recommendation=True,
        default_mixing_feature="search_group_name",
    ),
    "version_b_endpoint": ModelConfiguration(
        name="version_b",
        description="""
        Default model b:
        Takes the top {retrieval_limit} offers nearest the user iris (SQL)
        Takes the top {ranking_limit} recommendable (version_b endpoint) (NN)
        Applies diversification filter
        """,
        scorer=offer_scorer.OfferIrisRetrieval,
        scorer_order_columns="score",
        scorer_order_ascending=False,
        endpoint=RecommendationEndpoint(f"recommendation_version_b_{ENV_SHORT_NAME}"),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=10_000,
        ranking_order_query="is_geolocated DESC, booking_number DESC",
        ranking_limit=50,
        default_shuffle_recommendation=True,
        default_mixing_recommendation=True,
        default_mixing_feature="search_group_name",
    ),
    "top_offers": ModelConfiguration(
        name="top_offers",
        description="""
        Top offers:
        Takes top 100 offers
        Apply diversification filter
        """,
        scorer=offer_scorer.OfferIrisRetrieval,
        scorer_order_columns="booking_number",
        scorer_order_ascending=False,
        endpoint=DummyEndpoint(None),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=100,
        ranking_order_query="booking_number DESC",
        ranking_limit=100,
        default_shuffle_recommendation=True,
        default_mixing_recommendation=True,
        default_mixing_feature="search_group_name",
    ),
    "random": ModelConfiguration(
        name="random",
        description="""
        Random model:
        (mainly for testing purposes)
        Takes top 500 offers
        Shuffle and takes 50 randomly
        Apply diversification filter
        """,
        scorer=offer_scorer.OfferIrisRetrieval,
        scorer_order_columns="random",
        scorer_order_ascending=True,
        endpoint=DummyEndpoint(None),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=500,
        ranking_order_query="booking_number DESC",
        ranking_limit=50,
        default_shuffle_recommendation=True,
        default_mixing_recommendation=True,
        default_mixing_feature="search_group_name",
    ),
    "default_item": ModelConfiguration(
        name="default_item",
        description="""
        Default item :
        Takes the top {retrieval_limit} items (SQL)
        Takes the top {ranking_limit} recommendable (default endpoint) and lookup nearest offers (NN+SQL)
        Applies diversification filter
        """,
        scorer=offer_scorer.ItemRetrievalRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=RecommendationEndpoint(f"recommendation_default_{ENV_SHORT_NAME}"),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=50_000,
        ranking_order_query="user_km_distance ASC, item_score DESC",
        ranking_limit=50,
        default_shuffle_recommendation=True,
        default_mixing_recommendation=True,
        default_mixing_feature="search_group_name",
    ),
    "version_b_item": ModelConfiguration(
        name="version_b_item",
        description="""
        Item (version b):
        Takes the top {retrieval_limit} items (SQL)
        Takes the top {ranking_limit} recommendable (version_b endpoint) and lookup nearest offers (NN+SQL)
        Applies diversification filter
        """,
        scorer=offer_scorer.ItemRetrievalRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=RecommendationEndpoint(f"recommendation_version_b_{ENV_SHORT_NAME}"),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=50_000,
        ranking_order_query="user_km_distance ASC, item_score DESC",
        ranking_limit=50,
        default_shuffle_recommendation=True,
        default_mixing_recommendation=True,
        default_mixing_feature="search_group_name",
    ),
    "qpi_endpoint": ModelConfiguration(
        name="qpi_endpoint",
        description="""
        Cold Start + Item:
        Takes the top {retrieval_limit} items (SQL)
        Takes the top {ranking_limit} recommendable (QPI based endpoint) and lookup nearest offers (NN+SQL)
        """,
        scorer=offer_scorer.ItemRetrievalRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=QPIEndpoint(f"recommendation_cold_start_model_{ENV_SHORT_NAME}"),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=50_000,
        ranking_order_query="user_km_distance ASC, item_score DESC",
        ranking_limit=50,
    ),
}

RECOMMENDATION_ENDPOINTS = {
    # ColdStart : top offers
    # Recommendation : default endpoint, iris_retrieval
    "default": DefaultModel(
        model_name="default",
        cold_start_name="top_offers",
        cold_start_rules=InteractionRules(),
    ),
    # ColdStart : top offers
    # Recommendation : version b endpoint, iris_retrieval
    "version_b": DefaultModel(
        model_name="version_b_endpoint",
        cold_start_name="top_offers",
        cold_start_rules=InteractionRules(),
    ),
    # ColdStart : top offers
    # Recommendation : version b endpoint, item based retrieval
    "default_item": DefaultModel(
        model_name="default_item",
        cold_start_name="top_offers",
        cold_start_rules=InteractionRules(),
    ),
    # ColdStart : top offers
    # Recommendation : version b endpoint, item based retrieval
    "version_b_item": DefaultModel(
        model_name="default_item",
        cold_start_name="top_offers",
        cold_start_rules=InteractionRules(),
    ),
    # ColdStart : qpi based model
    # Recommendation : default endpoint, item based retrieval
    "default_qpi": DefaultModel(
        model_name="default_item",
        cold_start_name="qpi_endpoint",
        cold_start_rules=InteractionRules(),
    ),
    # ColdStart : top offers
    # Recommendation : top offers
    "top_offers": DefaultModel(
        model_name="top_offers",
        cold_start_name="top_offers",
        cold_start_rules=InteractionRules(None, None, None),
    ),
    # ColdStart : algo
    # Recommendation : algo
    "default_algo": DefaultModel(
        model_name="default",
        cold_start_name="default",
        cold_start_rules=InteractionRules(0, 0, 0),
    ),
    # ColdStart : qpi_algo
    # Recommendation : qpi_algo
    "cold_start": DefaultModel(
        model_name="qpi_endpoint",
        cold_start_name="qpi_endpoint",
        cold_start_rules=InteractionRules(None, None, None),
    ),
}
