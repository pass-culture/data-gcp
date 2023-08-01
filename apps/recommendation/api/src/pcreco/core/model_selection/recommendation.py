from pcreco.utils.env_vars import (
    ENV_SHORT_NAME,
)
import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer.recommendation import (
    RecommendationEndpoint,
    QPIEndpoint,
    DummyEndpoint,
)
from pcreco.core.scorer.retrieval_endpoint import (
    RetrievalEndpoint,
    UserRetrievalEndpoint,
)
from pcreco.core.model_selection.model_configuration import ModelConfiguration

default = ModelConfiguration(
    name="default",
    description="""
    Default model:
    Takes the top {retrieval_limit} offers nearest the user iris (SQL)
    Takes the top {ranking_limit} recommendable (default endpoint) (NN)
    Applies diversification filter
    """,
    scorer=offer_scorer.ItemRetrievalRanker,
    scorer_order_columns="order",
    scorer_order_ascending=True,
    endpoint=RecommendationEndpoint(f"recommendation_default_{ENV_SHORT_NAME}"),
    retrieval_order_query="booking_number DESC",
    retrieval_limit=20_000,
    ranking_order_query="item_score DESC",  # higher better
    ranking_limit=100,
    default_shuffle_recommendation=False,
    default_mixing_recommendation=True,
    default_mixing_feature="search_group_name",
)

version_b = ModelConfiguration(
    name="version_b",
    description="""
    Default model b:
    Takes the top {retrieval_limit} offers nearest the user iris (SQL)
    Takes the top {ranking_limit} recommendable (version_b endpoint) (NN)
    Applies diversification filter
    """,
    scorer=offer_scorer.ItemRetrievalRanker,
    scorer_order_columns="order",
    scorer_order_ascending=True,
    endpoint=RecommendationEndpoint(f"recommendation_version_b_{ENV_SHORT_NAME}"),
    retrieval_order_query="booking_number DESC",
    retrieval_limit=20_000,
    ranking_order_query="item_score DESC",  # higher better
    ranking_limit=100,
    default_shuffle_recommendation=False,
    default_mixing_recommendation=True,
    default_mixing_feature="search_group_name",
)

top_offers = ModelConfiguration(
    name="top_offers",
    description="""
        Top offers:
        Takes top 100 offers
        Apply diversification filter
        """,
    scorer=offer_scorer.ItemRetrievalRanker,
    # unused:
    scorer_order_columns="order",
    scorer_order_ascending=True,
    endpoint=DummyEndpoint(None),
    retrieval_order_query="booking_number DESC",
    retrieval_limit=500,
    ranking_order_query="booking_number DESC",
    ranking_limit=100,
    default_shuffle_recommendation=True,
    default_mixing_recommendation=True,
    default_mixing_feature="search_group_name",
)

retrieval_filter = ModelConfiguration(
    name="retrieval_filter",
    description="""
    Takes top 500 offers
    """,
    scorer=offer_scorer.DefaultRetrieval,
    endpoint=RetrievalEndpoint(f"recommendation_user_retrieval_{ENV_SHORT_NAME}"),
    retrieval_order_query=None,
    retrieval_limit=500,
    ranking_order_query="booking_number ASC",
    ranking_limit=100,
    scorer_order_columns="order",
    scorer_order_ascending=True,
    default_shuffle_recommendation=True,
    default_mixing_recommendation=True,
    default_mixing_feature="search_group_name",
)

random = ModelConfiguration(
    name="random",
    description="""
    Random model:
    (mainly for testing purposes)
    Takes top 500 offers
    Shuffle and takes 50 randomly
    Apply diversification filter
    """,
    scorer=offer_scorer.ItemRetrievalRanker,
    # unused:
    scorer_order_columns="order",
    scorer_order_ascending=True,
    endpoint=DummyEndpoint(None),
    retrieval_order_query="booking_number DESC",
    retrieval_limit=500,
    ranking_order_query="booking_number DESC",
    ranking_limit=50,
    default_shuffle_recommendation=True,
    default_mixing_recommendation=True,
    default_mixing_feature="search_group_name",
)

retrieval_reco = ModelConfiguration(
    name="retrieval_reco",
    description="""
    Recommendation retrieval model:
    Takes 500 personnalized offers
    Rank them by booking number
    """,
    scorer=offer_scorer.DefaultRetrieval,
    # unused:
    scorer_order_columns="order",
    scorer_order_ascending=True,
    endpoint=UserRetrievalEndpoint(f"recommendation_user_retrieval_{ENV_SHORT_NAME}"),
    retrieval_order_query=None,
    retrieval_limit=500,
    ranking_order_query="booking_number DESC",
    ranking_limit=100,
    default_shuffle_recommendation=True,
    default_mixing_recommendation=True,
    default_mixing_feature="search_group_name",
)
