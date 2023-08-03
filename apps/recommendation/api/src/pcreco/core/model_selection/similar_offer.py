from pcreco.utils.env_vars import ENV_SHORT_NAME

import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer.similar_offer import (
    DummyEndpoint,
    SimilarOfferEndpoint,
)
from pcreco.core.scorer.retrieval_endpoint import OfferRetrievalEndpoint
from pcreco.core.model_selection.model_configuration import ModelConfiguration

RETRIEVAL_LIMIT = 200
RANKING_LIMIT = 50


default = ModelConfiguration(
    name="item_v2",
    description="""
    Item model:
    Takes most similar ones (training based on clicks) (NN)
    Sort top 500 most similar (SQL)
    """,
    scorer=offer_scorer.SimilarOfferItemRanker,
    scorer_order_columns="order",
    scorer_order_ascending=True,
    endpoint=SimilarOfferEndpoint(f"similar_offers_version_b_{ENV_SHORT_NAME}"),
    retrieval_order_query=None,
    retrieval_limit=RETRIEVAL_LIMIT,
    ranking_order_query="item_score ASC",
    ranking_limit=RANKING_LIMIT,
)

retrieval_offer = ModelConfiguration(
    name="retrieval_offer",
    description="""
    Sim offers retrieval model:
    Takes 500 personnalized offers
    Rank them by booking number
    """,
    scorer=offer_scorer.DefaultRetrieval,
    scorer_order_columns="order",
    scorer_order_ascending=True,
    endpoint=OfferRetrievalEndpoint(f"recommendation_user_retrieval_{ENV_SHORT_NAME}"),
    retrieval_order_query=None,
    retrieval_limit=RETRIEVAL_LIMIT,
    ranking_order_query="item_score ASC",
    ranking_limit=RANKING_LIMIT,
)

cold_start = ModelConfiguration(
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
    ranking_order_query="item_score ASC",
    ranking_limit=RANKING_LIMIT,
)
random = ModelConfiguration(
    name="random",
    description="""
    Random model:
    (mainly for testing purposes)
    Takes top 1000 offers
    Shuffle and takes 20 randomly
    """,
    scorer=offer_scorer.ItemRetrievalRanker,
    scorer_order_columns="random",
    scorer_order_ascending=True,
    endpoint=DummyEndpoint(None),
    retrieval_order_query="booking_number DESC",
    retrieval_limit=100,
    ranking_order_query="booking_number DESC",
    ranking_limit=RANKING_LIMIT,
)
