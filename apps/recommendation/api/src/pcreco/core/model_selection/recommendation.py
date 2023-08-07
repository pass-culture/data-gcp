from pcreco.utils.env_vars import (
    ENV_SHORT_NAME,
)
from pcreco.core.scorer.retrieval_endpoint import (
    UserRetrievalEndpoint,
    FilterRetrievalEndpoint,
)
from pcreco.core.model_selection.model_configuration import (
    ModelConfiguration,
    diversification_on,
)
import pcreco.core.scorer.recommendable_offer as offer_scorer

retrieval_filter = ModelConfiguration(
    name="retrieval_filter",
    description="""""",
    scorer=offer_scorer.ScorerRetrieval,
    retrieval_limit=200,
    ranking_order_query="booking_number DESC",
    ranking_limit=100,
    diversification_params=diversification_on,
    endpoint=FilterRetrievalEndpoint(f"recommendation_user_retrieval_{ENV_SHORT_NAME}"),
)

retrieval_reco = ModelConfiguration(
    name="retrieval_reco",
    description="""""",
    scorer=offer_scorer.ScorerRetrieval,
    retrieval_limit=200,
    ranking_order_query="booking_number DESC",
    ranking_limit=100,
    diversification_params=diversification_on,
    endpoint=UserRetrievalEndpoint(f"recommendation_user_retrieval_{ENV_SHORT_NAME}"),
)
