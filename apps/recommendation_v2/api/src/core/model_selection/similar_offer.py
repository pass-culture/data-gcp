from utils.env_vars import ENV_SHORT_NAME
import core.scorer.offer as offer_scorer

import core.model_selection.endpoint.offer_retrieval as offer_retrieval


# from core.endpoint.ranking_endpoint import ModelRankingEndpoint
from core.model_selection.model_configuration import (
    ModelConfiguration,
    # diversification_off,
)

RANKING_LIMIT = 200

retrieval_offer = ModelConfiguration(
    name="similar_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_score ASC",
    ranking_limit_query=RANKING_LIMIT,
    # diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_retrieval_endpoint,
        offer_retrieval.semantic_offer_retrieval_endpoint,
        offer_retrieval.offer_filter_retrieval_endpoint,
    ]
    # ranking_endpoint=ModelRankingEndpoint(
    #     f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    # ),
)

retrieval_offer_version_b = ModelConfiguration(
    name="similar_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_score ASC",
    ranking_limit_query=RANKING_LIMIT,
    # diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_retrieval_version_b_endpoint,
        offer_retrieval.semantic_offer_retrieval_endpoint,
    ],
    # ranking_endpoint=ModelRankingEndpoint(
    #     f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    # ),
)


retrieval_cs_offer = ModelConfiguration(
    name="similar_cold_start_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_score ASC",
    ranking_limit_query=RANKING_LIMIT,
    # diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.semantic_offer_retrieval_endpoint,
        offer_retrieval.offer_filter_retrieval_endpoint,
    ],
    # ranking_endpoint=ModelRankingEndpoint(
    #     f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    # ),
)

retrieval_filter = ModelConfiguration(
    name="similar_offer_filter",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_score ASC",
    ranking_limit_query=RANKING_LIMIT,
    # diversification_params=diversification_off,
    retrieval_endpoints=[
        offer_retrieval.offer_filter_retrieval_endpoint,
    ],
    # ranking_endpoint=ModelRankingEndpoint(
    #     f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    # ),
)
