import core.scorer.recommendable_offer as offer_scorer
from core.scorer.similar_offer import (
    DummyEndpoint,
)
from core.model_selection.model_configuration import ModelConfiguration

SIMILAR_OFFER_ENDPOINTS = {
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
        retrieval_order_query="=booking_number DESC",
        retrieval_limit=1000,
        ranking_order_query="booking_number DESC",
        ranking_limit=20,
    )
}
