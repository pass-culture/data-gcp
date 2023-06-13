from dataclasses import dataclass
from pcreco.utils.env_vars import ENV_SHORT_NAME
import pcreco.core.scorer.similar_offer as similar_offer_scorer


@dataclass
class SimilarOfferDefaultModel:
    name: str
    scorer: similar_offer_scorer.SimilarOfferScorer
    retrieval_order_query: str
    retrieval_limit: int
    ranking_order_query: str
    ranking_limit: int
    endpoint_name: str = f"similar_offers_default_{ENV_SHORT_NAME}"


SIMILAR_OFFER_ENDPOINTS = {
    "default": SimilarOfferDefaultModel(
        name="default",
        scorer=similar_offer_scorer.OfferIrisRetrieval,
        retrieval_order_query="booking_number DESC",
        retrieval_limit=10_000,
        ranking_order_query="booking_number DESC",
        ranking_limit=20,
    ),
    "random": SimilarOfferDefaultModel(
        name="random",
        scorer=similar_offer_scorer.OfferIrisRandomScorer,
        retrieval_order_query="booking_number DESC",
        retrieval_limit=1000,
        ranking_order_query="booking_number DESC",
        ranking_limit=20,
    ),
    "item": SimilarOfferDefaultModel(
        name="item",
        scorer=similar_offer_scorer.ItemRetrievalRanker,
        retrieval_order_query="booking_number DESC",
        retrieval_limit=50_000,
        ranking_order_query="user_km_distance ASC, item_rank ASC",
        ranking_limit=20,
    ),
}
