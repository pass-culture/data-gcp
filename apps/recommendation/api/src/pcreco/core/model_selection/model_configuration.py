import pcreco.core.scorer.recommendable_offer as offer_scorer
from pcreco.core.scorer import ModelEndpoint
from dataclasses import dataclass
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.offer import Offer
from pcreco.core.user import User
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


@dataclass
class ModelFork:
    warm_start_model: ModelConfiguration
    cold_start_model: ModelConfiguration
    bookings_count: int = 2
    clicks_count: int = 25
    favorites_count: int = None

    def get_user_status(self, user: User):
        logger.info(
            f"{user.id}: user_model_status -> favorites {self.favorites_count} vs {user.favorites_count}"
        )
        if self.favorites_count is not None:
            if user.favorites_count >= self.favorites_count:
                return self.warm_start_model, "algo"
        logger.info(
            f"{user.id}: user_model_status -> booking {self.bookings_count} vs {user.bookings_count}"
        )
        if self.bookings_count is not None:
            if user.bookings_count >= self.bookings_count:
                return self.warm_start_model, "algo"
        logger.info(
            f"{user.id}: user_model_status -> clicks {self.clicks_count} vs {user.clicks_count}"
        )
        if self.clicks_count is not None:
            if user.clicks_count >= self.clicks_count:
                return self.warm_start_model, "algo"
        return self.cold_start_model, "cold_start"

    def get_offer_status(self, offer: Offer):
        logger.info(
            f"{offer.id}: offer_model_status -> booking {self.bookings_count} vs {offer.bookings_count}"
        )
        if self.favorites_count is not None:
            if offer.bookings_count >= self.bookings_count:
                return self.warm_start_model, "algo"
        return self.cold_start_model, "cold_start"
