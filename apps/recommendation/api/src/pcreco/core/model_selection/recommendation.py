from pcreco.utils.env_vars import (
    ENV_SHORT_NAME,
)
from pcreco.core.endpoint.retrieval_endpoint import (
    UserRetrievalEndpoint,
    FilterRetrievalEndpoint,
)
from pcreco.core.model_selection.model_configuration import (
    ModelConfiguration,
    diversification_on,
)
from pcreco.core.endpoint.ranking_endpoint import (
    ModelRankingEndpoint,
)
import pcreco.core.scorer.offer as offer_scorer

RETRIEVAL_LIMIT = 500
RANKING_LIMIT = 200

retrieval_filter = ModelConfiguration(
    name="recommendation_filter",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    retrieval_limit=RETRIEVAL_LIMIT,
    ranking_order_query="booking_number DESC",
    ranking_limit=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoint=FilterRetrievalEndpoint(
        endpoint_name=f"recommendation_user_retrieval_{ENV_SHORT_NAME}",
        fallback_endpoints=[
            f"recommendation_user_retrieval_version_b_{ENV_SHORT_NAME}",
            f"recommendation_semantic_retrieval_{ENV_SHORT_NAME}",
        ],
    ),
    ranking_endpoint=ModelRankingEndpoint(
        f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    ),
)


retrieval_reco = ModelConfiguration(
    name="recommendation_user",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    retrieval_limit=RETRIEVAL_LIMIT,
    ranking_order_query="booking_number DESC",
    ranking_limit=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoint=UserRetrievalEndpoint(
        endpoint_name=f"recommendation_user_retrieval_{ENV_SHORT_NAME}",
        fallback_endpoints=[
            f"recommendation_user_retrieval_version_b_{ENV_SHORT_NAME}"
        ],
    ),
    ranking_endpoint=ModelRankingEndpoint(
        f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    ),
)

retrieval_filter_version_b = ModelConfiguration(
    name="recommendation_filter",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    retrieval_limit=RETRIEVAL_LIMIT,
    ranking_order_query="booking_number DESC",
    ranking_limit=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoint=FilterRetrievalEndpoint(
        endpoint_name=f"recommendation_user_retrieval_version_b_{ENV_SHORT_NAME}",
        fallback_endpoints=[f"recommendation_user_retrieval_{ENV_SHORT_NAME}"],
    ),
    ranking_endpoint=ModelRankingEndpoint(
        f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    ),
)

retrieval_reco_version_b = ModelConfiguration(
    name="recommendation_user",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    retrieval_limit=RETRIEVAL_LIMIT,
    ranking_order_query="booking_number DESC",
    ranking_limit=RANKING_LIMIT,
    diversification_params=diversification_on,
    retrieval_endpoint=UserRetrievalEndpoint(
        endpoint_name=f"recommendation_user_retrieval_version_b_{ENV_SHORT_NAME}",
        fallback_endpoints=[f"recommendation_user_retrieval_{ENV_SHORT_NAME}"],
    ),
    ranking_endpoint=ModelRankingEndpoint(
        f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    ),
)
