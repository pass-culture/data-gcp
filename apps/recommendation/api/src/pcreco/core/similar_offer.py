from sqlalchemy import text
import json
from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.utils.db.db_connection import get_session
from pcreco.core.model_selection import (
    select_sim_model_params,
)
from pcreco.utils.env_vars import log_duration
from typing import List
import datetime
import time
import pytz


class SimilarOffer:
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParamsIn):
        self.user = user
        self.offer = offer
        self.model_params = select_sim_model_params(params_in.model_endpoint)
        self.params_in = params_in
        self.scorer = self.model_params.scorer(
            user=user, offer=offer, params_in=params_in, model_params=self.model_params
        )

    def get_scoring(self) -> List[str]:
        if self.offer.item_id is None:
            return []
        return self.scorer.get_scoring()

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
                        "group_id": self.scorer.model_params.name,
                        "model_name": self.scorer.model_display_name,
                        "model_version": self.scorer.model_version,
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
