from core.model_selection.endpoint import RankingEndpointName
from core.endpoint.ranking_endpoint import (
    ModelRankingEndpoint,
)

user_ranking_endpoint = ModelRankingEndpoint(
    endpoint_name=RankingEndpointName.recommendation_user_ranking,
    size=50,
)
