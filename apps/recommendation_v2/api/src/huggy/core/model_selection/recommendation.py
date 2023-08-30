import huggy.core.scorer.offer as offer_scorer

from huggy.core.model_selection.model_configuration import (
    ModelConfiguration,
    diversification_on,
)
import huggy.core.model_selection.endpoint.user_retrieval as user_retrieval
import huggy.core.model_selection.endpoint.user_ranking as user_ranking

RANKING_LIMIT = 100

retrieval_filter = ModelConfiguration(
    name="recommendation_filter",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoints=[user_retrieval.filter_retrieval_endpoint],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)


retrieval_reco = ModelConfiguration(
    name="recommendation_user",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoints=[
        user_retrieval.filter_retrieval_endpoint,
        user_retrieval.recommendation_retrieval_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)

retrieval_filter_version_b = ModelConfiguration(
    name="recommendation_filter",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoints=[user_retrieval.filter_retrieval_version_b_endpoint],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)

retrieval_reco_version_b = ModelConfiguration(
    name="recommendation_user",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    ranking_order_query="item_rank ASC",
    ranking_limit_query=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoints=[
        user_retrieval.filter_retrieval_version_b_endpoint,
        user_retrieval.recommendation_retrieval_version_b_endpoint,
    ],
    ranking_endpoint=user_ranking.user_ranking_endpoint,
)
