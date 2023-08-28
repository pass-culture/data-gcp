from sqlalchemy.orm import Session
from typing import List
import datetime
import time
import pytz

from huggy.schemas.user import User
from huggy.schemas.offer import Offer
from huggy.schemas.playlist_params import PlaylistParams

from huggy.core.model_engine import ModelEngine
from huggy.core.model_selection.model_configuration import ModelConfiguration
from huggy.core.model_selection import (
    select_sim_model_params,
)

from huggy.models.past_recommended_offers import PastSimilarOffers

from huggy.utils.env_vars import log_duration


class SimilarOffer(ModelEngine):
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParams):
        self.offer = offer
        super().__init__(user=user, params_in=params_in)

    def get_model_configuration(
        self, user: User, params_in: PlaylistParams
    ) -> ModelConfiguration:
        model_params, reco_origin = select_sim_model_params(
            params_in.model_endpoint, offer=self.offer
        )
        self.reco_origin = reco_origin
        return model_params

    def get_scorer(self):
        # init input
        for endpoint in self.model_params.retrieval_endpoints:
            endpoint.init_input(
                user=self.user, offer=self.offer, params_in=self.params_in
            )
        self.model_params.ranking_endpoint.init_input(
            user=self.user, params_in=self.params_in
        )
        return self.model_params.scorer(
            user=self.user,
            params_in=self.params_in,
            model_params=self.model_params,
            retrieval_endpoints=self.model_params.retrieval_endpoints,
            ranking_endpoint=self.model_params.ranking_endpoint,
        )

    def get_scoring(self, db: Session) -> List[str]:
        if self.offer.item_id is None:
            return []
        return super().get_scoring(db)

    def save_recommendation(self, db: Session, recommendations) -> None:
        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            for reco in recommendations:
                reco_offer = PastSimilarOffers(
                    user_id=self.user.user_id,
                    origin_offer_id=self.offer.offer_id,
                    offer_id=reco,
                    date=date,
                    group_id=self.model_params.name,
                    model_name=self.scorer.retrieval_endpoints[0].model_display_name,
                    model_version=self.scorer.retrieval_endpoints[0].model_version,
                    # reco_filters=json.dumps(self.params_in.json_input),
                    call_id=self.user.call_id,
                    venue_iris_id=self.offer.iris_id,
                )
                db.add(reco_offer)
            db.commit()
            log_duration(f"save_recommendations for {self.user.user_id}", start)
