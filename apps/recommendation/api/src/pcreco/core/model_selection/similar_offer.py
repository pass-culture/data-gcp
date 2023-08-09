from pcreco.utils.env_vars import ENV_SHORT_NAME
import pcreco.core.scorer.offer as offer_scorer
from pcreco.core.endpoint.retrieval_endpoint import (
    OfferRetrievalEndpoint,
    OfferFilterRetrievalEndpoint,
)
from pcreco.core.endpoint.ranking_endpoint import ModelRankingEndpoint
from pcreco.core.model_selection.model_configuration import (
    ModelConfiguration,
    diversification_off,
)

retrieval_offer = ModelConfiguration(
    name="similar_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    retrieval_limit=200,
    ranking_order_query="item_score ASC",
    ranking_limit=100,
    diversification_params=diversification_off,
    retrieval_endpoint=OfferRetrievalEndpoint(
        f"recommendation_user_retrieval_{ENV_SHORT_NAME}"
    ),
    ranking_endpoint=ModelRankingEndpoint(
        f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    ),
)

retrieval_cs_offer = ModelConfiguration(
    name="similar_cold_start_offer_model",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    retrieval_limit=200,
    ranking_order_query="item_score ASC",
    ranking_limit=100,
    diversification_params=diversification_off,
    retrieval_endpoint=OfferRetrievalEndpoint(
        f"recommendation_semantic_retrieval_{ENV_SHORT_NAME}"
    ),
    ranking_endpoint=ModelRankingEndpoint(
        f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    ),
)

retrieval_filter = ModelConfiguration(
    name="similar_offer_filter",
    description="""""",
    scorer=offer_scorer.OfferScorer,
    retrieval_limit=200,
    ranking_order_query="item_score ASC",
    ranking_limit=100,
    diversification_params=diversification_off,
    retrieval_endpoint=OfferFilterRetrievalEndpoint(
        f"recommendation_user_retrieval_{ENV_SHORT_NAME}"
    ),
    ranking_endpoint=ModelRankingEndpoint(
        f"recommendation_user_ranking_{ENV_SHORT_NAME}"
    ),
)
