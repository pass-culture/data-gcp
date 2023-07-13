import core.scorer.recommendable_offer as offer_scorer
from core.scorer import ModelEndpoint
from dataclasses import dataclass
from schemas.playlist_params import PlaylistParamsRequest
# from loguru import logger


@dataclass
class ModelConfiguration:
    name: str
    description: str
    scorer: offer_scorer.ScorerRetrieval
    scorer_order_columns: str
    scorer_order_ascending: bool
    endpoint: ModelEndpoint
    retrieval_order_query: str
    retrieval_limit: int
    ranking_order_query: str
    ranking_limit: int
    default_shuffle_recommendation: bool = False
    default_mixing_recommendation: bool = False
    default_mixing_feature: str = "search_group_name"