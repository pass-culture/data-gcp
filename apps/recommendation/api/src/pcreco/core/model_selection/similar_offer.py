from pcreco.utils.env_vars import ENV_SHORT_NAME

import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer.similar_offer import SimilarOfferEndpoint, DummyEndpoint, SimilarOfferV2Endpoint
from pcreco.core.model_selection.model_configuration import ModelConfiguration

SIMILAR_OFFER_ENDPOINTS = {
    "default": ModelConfiguration(
        name="default",
        description="""
        Default model:
        Takes the top {retrieval_limit} offers nearest the user iris (SQL)
        Takes the top {ranking_limit} most similar ones (training based on clicks) (NN)
        """,
        scorer=offer_scorer.OfferIrisRetrieval,
        scorer_order_columns="booking_number",
        scorer_order_ascending=False,
        endpoint=SimilarOfferEndpoint(f"similar_offers_default_{ENV_SHORT_NAME}"),
        retrieval_order_query="booking_number DESC",
        retrieval_limit=10_000,
        ranking_order_query="booking_number DESC",
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
    "item": ModelConfiguration(
        name="item",
        description="""
        Item model:
        Takes the top {retrieval_limit} items (SQL)  
        Takes most similar ones (training based on clicks) (NN)
        Sort top 500 most similar, by distance range and similarity score (SQL)
        """,
        scorer=offer_scorer.ItemRetrievalRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=SimilarOfferEndpoint(f"similar_offers_default_{ENV_SHORT_NAME}"),
        retrieval_order_query="RANDOM() ASC",
        retrieval_limit=30_000,
        ranking_order_query="item_score DESC",
        ranking_limit=20,
    ),
    "item_v2": ModelConfiguration(
        name="item_v2",
        description="""
        Item model:
        Takes most similar ones (training based on clicks) (NN)
        Sort top 500 most similar, by distance range and similarity score (SQL)
        """,
        scorer=offer_scorer.SimilarOfferItemRanker,
        scorer_order_columns="order",
        scorer_order_ascending=True,
        endpoint=SimilarOfferV2Endpoint(f"similar_offers_default_{ENV_SHORT_NAME}"),
        retrieval_order_query=None,
        retrieval_limit=500,
        ranking_order_query="item_score DESC",
        ranking_limit=20,
    ),
}
