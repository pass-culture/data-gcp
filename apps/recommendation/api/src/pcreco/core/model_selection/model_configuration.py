import pcreco.core.scorer.offer as offer_scorer
from pcreco.core.endpoint.retrieval_endpoint import RetrievalEndpoint
from pcreco.core.endpoint.ranking_endpoint import RankingEndpoint
from dataclasses import dataclass
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.offer import Offer
from pcreco.core.user import User
import typing as t
import copy


@dataclass
class DiversificationParams:
    is_active: bool
    is_reco_shuffled: bool
    mixing_features: str
    order_column: str
    order_ascending: bool


diversification_on = DiversificationParams(
    is_active=True,
    is_reco_shuffled=False,
    mixing_features="search_group_name",
    order_column="offer_output",
    order_ascending=False,
)

diversification_off = DiversificationParams(
    is_active=False,
    is_reco_shuffled=False,
    mixing_features="search_group_name",
    order_column="offer_output",
    order_ascending=False,
)


@dataclass
class ModelConfiguration:
    name: str
    description: str
    scorer: offer_scorer.OfferScorer
    retrieval_endpoints: t.List[RetrievalEndpoint]
    ranking_endpoint: RankingEndpoint
    ranking_order_query: str
    ranking_limit_query: int
    diversification_params: DiversificationParams

    def get_diversification_params(
        self, params_in: PlaylistParamsIn
    ) -> DiversificationParams:
        """
        Overwrite default params
        """
        if params_in.is_reco_mixed is not None:
            self.diversification_params.is_active = params_in.is_reco_mixed

        if params_in.is_reco_shuffled is not None:
            self.diversification_params.is_reco_shuffled = params_in.is_reco_shuffled

        if params_in.mixing_features is not None:
            self.diversification_params.mixing_features = params_in.mixing_features

        return self.diversification_params


@dataclass
class ModelFork:
    warm_start_model: ModelConfiguration
    cold_start_model: ModelConfiguration
    bookings_count: int = 2
    clicks_count: int = 25
    favorites_count: int = None

    def get_user_status(self, user: User):
        """Get model status based on User interactions"""
        if not user.found:
            return copy.deepcopy(self.cold_start_model), "unknown"

        if self.favorites_count is not None:
            if user.favorites_count >= self.favorites_count:
                return copy.deepcopy(self.warm_start_model), "algo"

        if self.bookings_count is not None:
            if user.bookings_count >= self.bookings_count:
                return copy.deepcopy(self.warm_start_model), "algo"

        if self.clicks_count is not None:
            if user.clicks_count >= self.clicks_count:
                return copy.deepcopy(self.warm_start_model), "algo"
        return copy.deepcopy(self.cold_start_model), "cold_start"

    def get_offer_status(self, offer: Offer):
        """Get model status based on Offer interactions"""
        if not offer.found:
            return copy.deepcopy(self.cold_start_model), "unknown"
        if self.bookings_count is not None:
            if offer.bookings_count >= self.bookings_count:
                return copy.deepcopy(self.warm_start_model), "algo"
        return copy.deepcopy(self.cold_start_model), "cold_start"
