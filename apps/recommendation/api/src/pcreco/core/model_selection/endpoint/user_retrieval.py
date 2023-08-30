from pcreco.core.model_selection.endpoint import RetrievalEndpointName
from pcreco.core.endpoint.retrieval_endpoint import (
    RecommendationRetrievalEndpoint,
    FilterRetrievalEndpoint,
)

filter_retrieval_endpoint = FilterRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_user_retrieval,
    size=500,
    fallback_endpoints=[
        RetrievalEndpointName.recommendation_user_retrieval_version_b,
    ],
)

recommendation_retrieval_endpoint = RecommendationRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_user_retrieval,
    size=500,
    fallback_endpoints=[RetrievalEndpointName.recommendation_user_retrieval_version_b],
)


filter_retrieval_version_b_endpoint = FilterRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_user_retrieval_version_b,
    size=500,
    fallback_endpoints=[
        RetrievalEndpointName.recommendation_user_retrieval,
    ],
)

recommendation_retrieval_version_b_endpoint = RecommendationRetrievalEndpoint(
    endpoint_name=RetrievalEndpointName.recommendation_user_retrieval_version_b,
    size=500,
    fallback_endpoints=[RetrievalEndpointName.recommendation_user_retrieval],
)
