from pcreco.utils.env_vars import ENV_SHORT_NAME

import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer.similar_offer import (
    DummyEndpoint,
    SimilarOfferEndpoint,
)
from pcreco.core.model_selection.model_configuration import ModelConfiguration

SIMILAR_OFFER_ENDPOINTS = {
    "default": ModelConfiguration(
        name="item_v2",
        description="""
        Item model:
        Takes most similar ones (training based on clicks) (NN)
        Sort top 500 most similar (SQL)
        """,
        scorer=offer_scorer.SimilarOfferItemRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=SimilarOfferEndpoint(f"similar_offers_default_{ENV_SHORT_NAME}"),
        retrieval_order_query=None,
        retrieval_limit=500,
        ranking_order_query="item_score DESC",
        ranking_limit=20,
    ),
    "version_b": ModelConfiguration(
        name="version_b",
        description="""
        Item model:
        Takes most similar ones (training based on two-towers) (NN)
        Sort top 500 most similar (SQL)
        """,
        scorer=offer_scorer.SimilarOfferItemRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=SimilarOfferEndpoint(f"similar_offers_version_b_{ENV_SHORT_NAME}"),
        retrieval_order_query=None,
        retrieval_limit=500,
        ranking_order_query="item_score DESC",
        ranking_limit=20,
    ),
    "version_c": ModelConfiguration(
        name="version_c",
        description="""
        Item model:
        Takes most similar ones
        Sort top 500 most similar, by distance range and similarity score (SQL)
        """,
        scorer=offer_scorer.SimilarOfferItemRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=SimilarOfferEndpoint(f"similar_offers_version_c_{ENV_SHORT_NAME}"),
        retrieval_order_query=None,
        retrieval_limit=500,
        ranking_order_query="user_km_distance ASC, item_score DESC",
        ranking_limit=20,
    ),
    "cold_start": ModelConfiguration(
        name="cold_start",
        description="""
        Item model:
        Takes most similar ones based on cold_start
        Sort top 500 most similar (SQL)
        """,
        scorer=offer_scorer.SimilarOfferItemRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=SimilarOfferEndpoint(f"similar_offers_cold_start_{ENV_SHORT_NAME}"),
        retrieval_order_query=None,
        retrieval_limit=500,
        ranking_order_query="item_score DESC",
        ranking_limit=20,
    ),
    "random": ModelConfiguration(
        name="random",
        description="""
        Random model:
        (mainly for testing purposes)
        Takes top 1000 offers
        Shuffle and takes 20 randomly
        """,
        scorer=offer_scorer.OfferIrisRetrieval,
        scorer_order_columns="random",
        scorer_order_ascending=True,
        endpoint=DummyEndpoint(None),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=1000,
        ranking_order_query="booking_number DESC",
        ranking_limit=20,
    ),
}
