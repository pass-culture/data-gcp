from sqlalchemy import text
from schemas.user import User
from schemas.offer import Offer
from schemas.playlist_params import PlaylistParamsRequest
from models.past_recommended_offers import PastSimilarOffers

from sqlalchemy.orm import Session

from typing import List
import datetime
import time
import pytz


class SimilarOffer:
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParamsRequest):
        self.offer = offer
        self.user = user
        self.params_in = params_in

    def get_scoring(self) -> List[str]:
        # if self.offer.item_id is None:
        #     return []
        user_recommendation = {
            # FOR TEST ONLY
            "isbn-1": {
                "id": 1,
                "item_id": "isbn-1",
                "venue_id": "11",
                "user_distance": 10,
                "booking_number": 3,
                "category": "A",
                "subcategory_id": "EVENEMENT_CINE",
                "search_group_name": "CINEMA",
                "is_geolocated": False,
                "random": 1,
            },
            "movie-3": {
                "id": 3,
                "item_id": "movie-3",
                "venue_id": "33",
                "user_distance": 20,
                "booking_number": 10,
                "category": "C",
                "subcategory_id": "EVENEMENT_CINE",
                "search_group_name": "CINEMA",
                "is_geolocated": False,
                "random": 3,
            },
        }
        return user_recommendation

    def save_recommendation(self, db: Session, recommendations) -> None:
        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            for item, offer_infos in recommendations.items():
                reco_offer = PastSimilarOffers(
                    call_id=self.user.call_id,
                    user_id=self.user.user_id,
                    origin_offer_id=self.offer.offer_id,
                    offer_id=offer_infos["id"],
                    date=date,
                    group_id="group_id",
                    model_name="model_name",
                    model_version="model_version",
                    reco_filters=self.params_in,
                    venue_iris_id=self.offer.iris_id,
                )
                db.add(reco_offer)
            db.commit()
            # log_duration(f"save_recommendations for {self.user.id}", start)
