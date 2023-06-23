from sqlalchemy import text
import json
from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.model_selection.model_configuration import ModelConfiguration
from pcreco.utils.db.db_connection import get_session
from pcreco.core.model_engine import ModelEngine
from pcreco.core.model_selection import (
    select_sim_model_params,
)
from pcreco.utils.env_vars import log_duration
from typing import List
import datetime
import time
import pytz


class SimilarOffer(ModelEngine):
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParamsIn):
        self.offer = offer
        super().__init__(user=user, params_in=params_in)

    def get_scorer(self):
        # init input
        self.model_params.endpoint.init_input(self.offer)
        return self.model_params.scorer(
            user=self.user,
            params_in=self.params_in,
            model_params=self.model_params,
            model_endpoint=self.model_params.endpoint,
        )

    def get_model_configuration(
        self, user: User, params_in: PlaylistParamsIn
    ) -> ModelConfiguration:
        model_params = select_sim_model_params(params_in.model_endpoint)
        self.reco_origin = "default"
        return model_params

    def get_scoring(self) -> List[str]:
        if self.offer.item_id is None:
            return []
        return super().get_scoring()

    def save_recommendation(self, recommendations) -> None:
        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            rows = []

            for offer_id in recommendations:
                rows.append(
                    {
                        "user_id": self.user.id,
                        "origin_offer_id": self.offer.id,
                        "offer_id": offer_id,
                        "date": date,
                        "group_id": self.model_params.name,
                        "model_name": self.scorer.model_endpoint.model_display_name,
                        "model_version": self.scorer.model_endpoint.model_version,
                        "reco_filters": json.dumps(self.params_in.json_input),
                        "call_id": self.user.call_id,
                        "venue_iris_id": self.offer.iris_id,
                    }
                )

            connection = get_session()
            connection.execute(
                text(
                    """
                    INSERT INTO public.past_similar_offers (user_id, origin_offer_id, offer_id, date, group_id, model_name, model_version, reco_filters, call_id, venue_iris_id)
                    VALUES (:user_id, :origin_offer_id, :offer_id, :date, :group_id, :model_name, :model_version, :reco_filters, :call_id, :venue_iris_id)
                    """
                ),
                rows,
            )
            log_duration(f"save_recommendations for {self.user.id}", start)
