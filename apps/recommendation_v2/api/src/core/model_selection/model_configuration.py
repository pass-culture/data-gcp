import core.scorer.offer as offer_scorer
from core.endpoint.retrieval_endpoint import RetrievalEndpoint

# from endpoint.ranking_endpoint import RankingEndpoint
from dataclasses import dataclass
from schemas.playlist_params import PlaylistParams
from schemas.offer import Offer
from schemas.user import User


@dataclass
class ModelConfiguration:
    name: str
    description: str
    scorer: offer_scorer.OfferScorer
    retrieval_endpoint: RetrievalEndpoint
    retrieval_limit: int
    # ranking_endpoint: RankingEndpoint
    ranking_order_query: str
    ranking_limit: int
    # diversification_params: DiversificationParams

    # def get_diversification_params(
    #     self, params_in: PlaylistParamsIn
    # ) -> DiversificationParams:
    #     """
    #     Overwrite default params
    #     """
    #     if params_in.is_reco_mixed is not None:
    #         self.diversification_params.is_active = params_in.is_reco_mixed

    #     if params_in.is_reco_shuffled is not None:
    #         self.diversification_params.is_reco_shuffled = params_in.is_reco_shuffled

    #     if params_in.mixing_features is not None:
    #         self.diversification_params.mixing_features = params_in.mixing_features

    #     return self.diversification_params


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
            return self.cold_start_model, "unknown"

        if self.favorites_count is not None:
            if user.favorites_count >= self.favorites_count:
                return self.warm_start_model, "algo"

        if self.bookings_count is not None:
            if user.bookings_count >= self.bookings_count:
                return self.warm_start_model, "algo"

        if self.clicks_count is not None:
            if user.clicks_count >= self.clicks_count:
                return self.warm_start_model, "algo"
        return self.cold_start_model, "cold_start"

    def get_offer_status(self, offer: Offer):
        """Get model status based on Offer interactions"""
        if not offer.found:
            return self.cold_start_model, "unknown"
        if self.bookings_count is not None:
            if offer.cnt_bookings >= self.bookings_count:
                return self.warm_start_model, "algo"
        return self.cold_start_model, "cold_start"
