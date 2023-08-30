import pcreco.core.scorer.offer as offer_scorer
from pcreco.core.model_selection.model_configuration import (
    ModelConfiguration,
    diversification_off,
)
import pcreco.core.model_selection.endpoint.offer_retrieval as offer_retrieval
import pcreco.core.model_selection.endpoint.user_ranking as user_ranking

RANKING_LIMIT = 100

retrieval_offer = ModelConfiguration(
    name="similar_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=100,
    diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_retrieval_endpoint,
        offer_retrieval.semantic_offer_retrieval_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)

retrieval_offer_version_b = ModelConfiguration(
    name="similar_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=100,
    diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_retrieval_version_b_endpoint,
        offer_retrieval.semantic_offer_retrieval_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)

retrieval_cs_offer = ModelConfiguration(
    name="similar_cold_start_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=100,
    diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.semantic_offer_retrieval_endpoint,
        offer_retrieval.offer_filter_retrieval_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)

retrieval_filter = ModelConfiguration(
    name="similar_offer_filter",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=100,
    diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_filter_retrieval_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)
