from sqlalchemy.orm import Session
from typing import List
import datetime
import time
import random
import pytz

from schemas.user import User
from schemas.offer import Offer
from schemas.item import Item

from models.past_recommended_offers import PastSimilarOffers

from crud.offer import get_nearest_offer, get_non_recommendable_items


class SimilarOffer:
    def __init__(self, user: User, offer: Offer):
        self.offer = offer
        self.user = user

    def get_scoring(self, db: Session) -> List[dict]:
        if self.offer.item_id is None:
            return []

        # 1. get recommendable items
        recommendable_items = {
            # "product-54666": {
            #     "item_id": "product-54666",
            #     "user_distance": 10,
            #     "booking_number": 3,
            #     "category": "A",
            #     "subcategory_id": "EVENEMENT_CINE",
            #     "search_group_name": "CINEMA",
            #     "random": 1,
            # },
            # FOR TEST ONLY
            "isbn-1": {
                "item_id": "isbn-1",
                "user_distance": 10,
                "booking_number": 3,
                "category": "A",
                "subcategory_id": "EVENEMENT_CINE",
                "search_group_name": "CINEMA",
                "random": 1,
            },
            "isbn-2": {
                "item_id": "isbn-2",
                "user_distance": 10,
                "booking_number": 3,
                "category": "A",
                "subcategory_id": "EVENEMENT_CINE",
                "search_group_name": "CINEMA",
                "random": 1,
            },
            "movie-3": {
                "item_id": "movie-3",
                "user_distance": 20,
                "booking_number": 10,
                "category": "C",
                "subcategory_id": "EVENEMENT_CINE",
                "search_group_name": "CINEMA",
                "random": 3,
            },
        }

        selected_items = [Item(item_id=item) for item in recommendable_items.keys()]
        
        non_recommendable_items = get_non_recommendable_items(db, self.user)
        
        selected_items = [item for item in selected_items if item not in non_recommendable_items] # Remove non recommendable items

        # # 2. score items
        predicted_items = [Item(item_id=item.item_id, item_score=random.random()) for item in selected_items]

        # # 3. Ranking items and retrieve nearest offer
        output_list = []
        for item in predicted_items:
            output_items = {}
            nearest_offer = get_nearest_offer(
                db, self.user, Item(item.item_id, item.item_score)
            )
            if len(nearest_offer) > 0:
                output_items["item_id"] = item.item_id
                output_items["score"] = item.item_score
                output_items["nearest_offer_id"] = nearest_offer[0].offer_id
                output_items["user_distance"] = nearest_offer[0].user_distance
                output_list.append(output_items)
        
        # Sort offers depending on a parameter 
        # Limit displayed offers depended on a

        return output_list
        

    def save_recommendation(self, db: Session, recommendations) -> None:
        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            for reco in recommendations:
                reco_offer = PastSimilarOffers(
                    call_id=self.user.call_id,
                    user_id=self.user.user_id,
                    origin_offer_id=self.offer.offer_id,
                    offer_id=reco["nearest_offer_id"],
                    date=date,
                    group_id="group_id",  # temp
                    model_name="model_name",  # temp
                    model_version="model_version",  # temp
                    venue_iris_id=self.offer.iris_id,
                )
                db.add(reco_offer)
            db.commit()
            # log_duration(f"save_recommendations for {self.user.id}", start)
