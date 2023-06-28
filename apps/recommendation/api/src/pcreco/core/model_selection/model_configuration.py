import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer import ModelEndpoint
from dataclasses import dataclass
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from loguru import logger


@dataclass
class DiversificationParams:
    is_active: bool
    is_reco_shuffled: bool
    mixing_features: str


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

    def get_diversification_params(
        self, params_in: PlaylistParamsIn
    ) -> DiversificationParams:

        if params_in.is_reco_mixed is not None:
            is_active = params_in.is_reco_mixed
        else:
            is_active = self.default_mixing_recommendation

        if params_in.is_reco_shuffled is not None:
            is_reco_shuffled = params_in.is_reco_shuffled
        else:
            is_reco_shuffled = self.default_shuffle_recommendation

        if params_in.mixing_features is not None:
            mixing_features = params_in.mixing_features
        else:
            mixing_features = self.default_mixing_feature

        return DiversificationParams(
            is_active=is_active,
            is_reco_shuffled=is_reco_shuffled,
            mixing_features=mixing_features,
        )
