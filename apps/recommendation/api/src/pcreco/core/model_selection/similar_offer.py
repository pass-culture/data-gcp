from pcreco.utils.env_vars import ENV_SHORT_NAME
import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer.retrieval_endpoint import (
    OfferRetrievalEndpoint,
    OfferFilterRetrievalEndpoint,
)
from pcreco.core.model_selection.model_configuration import (
    ModelConfiguration,
    diversification_off,
)

retrieval_offer = ModelConfiguration(
    name="retrieval_offer",
    description="""""",
    scorer=offer_scorer.ScorerRetrieval,
    retrieval_limit=200,
    ranking_order_query="item_score ASC",
    ranking_limit=20,
    diversification_params=diversification_off,
    endpoint=OfferRetrievalEndpoint(f"recommendation_user_retrieval_{ENV_SHORT_NAME}"),
)


retrieval_filter = ModelConfiguration(
    name="retrieval_filter",
    description="""""",
    scorer=offer_scorer.ScorerRetrieval,
    retrieval_limit=200,
    ranking_order_query="item_score ASC",
    ranking_limit=20,
    diversification_params=diversification_off,
    endpoint=OfferFilterRetrievalEndpoint(
        f"recommendation_user_retrieval_{ENV_SHORT_NAME}"
    ),
)
