from typing import List
from abc import ABC, abstractmethod
from huggy.schemas.user import User
from huggy.schemas.playlist_params import PlaylistParams
from huggy.core.model_selection.model_configuration import ModelConfiguration
from huggy.utils.mixing import order_offers_by_score_and_diversify_features
from huggy.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
)

from loguru import logger
from huggy.core.scorer.offer import OfferScorer
from sqlalchemy.orm import Session


class ModelEngine(ABC):
    def __init__(self, user: User, params_in: PlaylistParams):
        self.user = user
        self.params_in = params_in
        # Get model (cold_start or algo)
        self.model_params = self.get_model_configuration(user, params_in)
        self.scorer = self.get_scorer()

    @abstractmethod
    def get_model_configuration(
        self, user: User, params_in: PlaylistParams
    ) -> ModelConfiguration:
        pass

    def get_scorer(self) -> OfferScorer:
        # init user_input
        for endpoint in self.model_params.retrieval_endpoints:
            endpoint.init_input(user=self.user, params_in=self.params_in)
        self.model_params.ranking_endpoint.init_input(
            user=self.user, params_in=self.params_in
        )
        # get scorer
        return self.model_params.scorer(
            user=self.user,
            params_in=self.params_in,
            model_params=self.model_params,
            retrieval_endpoints=self.model_params.retrieval_endpoints,
            ranking_endpoint=self.model_params.ranking_endpoint,
        )

    def get_scoring(self, db: Session) -> List[str]:
        """
        Returns a list of offer_id to be send to the user
        Depends of the scorer method.
        """
        scored_offers = self.scorer.get_scoring(db)
        if len(scored_offers) == 0:
            return []

        diversification_params = self.model_params.get_diversification_params(
            self.params_in
        )
        logger.info(
            f"{self.user.user_id}: get_scoring -> diversification active: {diversification_params.is_active}, shuffle: {diversification_params.is_reco_shuffled}, mixing key: {diversification_params.mixing_features}"
        )

        # apply diversification filter
        if diversification_params.is_active:
            scored_offers = order_offers_by_score_and_diversify_features(
                offers=scored_offers,
                score_column=diversification_params.order_column,
                score_order_ascending=diversification_params.order_ascending,
                shuffle_recommendation=diversification_params.is_reco_shuffled,
                feature=diversification_params.mixing_features,
                nb_reco_display=NUMBER_OF_RECOMMENDATIONS,
            )

        scoring_size = min(len(scored_offers), NUMBER_OF_RECOMMENDATIONS)
        # save_context(
        #     offers=scored_offers,
        #     call_id=self.user.call_id,
        #     context=self.model_params.name,
        #     user=self.user,
        # )

        return [offer.offer_id for offer in scored_offers][:scoring_size]
