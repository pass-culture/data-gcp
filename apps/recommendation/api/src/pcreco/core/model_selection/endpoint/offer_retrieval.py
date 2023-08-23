from pcreco.core.model_selection.endpoint import RetrievalEndpointName
from pcreco.core.endpoint.retrieval_endpoint import (
    OfferRetrievalEndpoint,
    OfferFilterRetrievalEndpoint,
)

RETRIEVAL_LIMIT = 500


offer_retrieval_endpoint = OfferRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_user_retrieval,
    size=RETRIEVAL_LIMIT,
    fallback_endpoints=[
        RetrievalEndpointName.recommendation_user_retrieval_version_b,
    ],
)

offer_filter_retrieval_endpoint = OfferFilterRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_user_retrieval,
    size=100,
    fallback_endpoints=[RetrievalEndpointName.recommendation_user_retrieval_version_b],
)

semantic_offer_retrieval_endpoint = OfferRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_semantic_retrieval,
    size=RETRIEVAL_LIMIT,
)

offer_retrieval_version_b_endpoint = OfferRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_user_retrieval_version_b,
    size=RETRIEVAL_LIMIT,
    fallback_endpoints=[
        RetrievalEndpointName.recommendation_user_retrieval,
    ],
)
