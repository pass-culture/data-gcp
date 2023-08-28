import huggy.core.scorer.offer as offer_scorer

import huggy.core.model_selection.endpoint.offer_retrieval as offer_retrieval
import huggy.core.model_selection.endpoint.user_ranking as user_ranking
from huggy.core.model_selection.model_configuration import (
    ModelConfiguration,
    diversification_off,
)

from huggy.utils.env_vars import ENV_SHORT_NAME

RANKING_LIMIT = 100

retrieval_offer = ModelConfiguration(
    name="similar_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=RANKING_LIMIT,
    diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_retrieval_endpoint,
        offer_retrieval.semantic_offer_retrieval_endpoint,
        offer_retrieval.offer_filter_retrieval_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)

retrieval_offer_version_b = ModelConfiguration(
    name="similar_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=RANKING_LIMIT,
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
    ranking_limit_query=RANKING_LIMIT,
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
    ranking_limit_query=RANKING_LIMIT,
    diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_filter_retrieval_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)
