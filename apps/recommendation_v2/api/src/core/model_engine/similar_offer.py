from sqlalchemy import text
import json
from schemas.user import User
from schemas.offer import Offer
from schemas.playlist_params import PlaylistParamsRequest
from models.past_recommended_offers import PastSimilarOffers
from core.model_selection.model_configuration import ModelConfiguration
# from pcreco.utils.db.db_connection import get_session
from core.model_engine import ModelEngine
from core.model_selection import (
    select_sim_model_params,
)
from sqlalchemy.orm import Session
# from pcreco.utils.env_vars import log_duration
from typing import List
import datetime
import time
import pytz


class SimilarOffer(ModelEngine):
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParamsRequest):
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
        self, user: User, params_in: PlaylistParamsRequest
    ) -> ModelConfiguration:
        model_params = select_sim_model_params(params_in.model_endpoint)
        self.reco_origin = "default"
        return model_params

    def get_scoring(self) -> List[str]:
        if self.offer.item_id is None:
            return []
        return super().get_scoring()
    
    def save_recommendation(self, db: Session, recommendations) -> None:
        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            for offer_id in recommendations:
                reco_offer = PastSimilarOffers(
                    call_id=self.user.call_id,
                    user_id=self.user.id,
                    origin_offer_id=self.offer.id, 
                    offer_id=offer_id,
                    date=date,
                    group_id=self.model_params.name,
                    model_name=self.scorer.model_endpoint.model_display_name,
                    model_version=self.scorer.model_endpoint.model_version,
                    reco_filters=self.params_in,
                    venue_iris_id=self.offer.iris_id
                    )
                db.add(reco_offer)
            db.commit()
            # log_duration(f"save_recommendations for {self.user.id}", start)