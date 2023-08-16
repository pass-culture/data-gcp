from sqlalchemy.orm import Session
from typing import List
import datetime
import time
import pytz

from schemas.user import User
from schemas.item import Item

from models.past_recommended_offers import PastRecommendedOffers

from crud.offer import get_nearest_offer


class Recommendation:
    def __init__(self, user: User):
        self.user = user

    def get_scoring(self, db: Session) -> List[dict]:

        # 1. get recommendable items
        recommendable_items = {
            "product-54666": {
                "item_id": "product-54666",
                "user_distance": 10,
                "booking_number": 3,
                "category": "A",
                "subcategory_id": "EVENEMENT_CINE",
                "search_group_name": "CINEMA",
                "random": 1,
            },
            # FOR TEST ONLY
            # "isbn-1": {
            #     "item_id": "isbn-1",
            #     "user_distance": 10,
            #     "booking_number": 3,
            #     "category": "A",
            #     "subcategory_id": "EVENEMENT_CINE",
            #     "search_group_name": "CINEMA",
            #     "random": 1,
            # },
            # "isbn-2": {
            #     "item_id": "isbn-2",
            #     "user_distance": 10,
            #     "booking_number": 3,
            #     "category": "A",
            #     "subcategory_id": "EVENEMENT_CINE",
            #     "search_group_name": "CINEMA",
            #     "random": 1,
            # },
            # "movie-3": {
            #     "item_id": "movie-3",
            #     "user_distance": 20,
            #     "booking_number": 10,
            #     "category": "C",
            #     "subcategory_id": "EVENEMENT_CINE",
            #     "search_group_name": "CINEMA",
            #     "random": 3,
            # },
        }

        selected_items = list(recommendable_items.keys())

        # 2. score items
        predicted_items = [
            Item(item_id="product-54666", item_score=10),
            # Item(item_id="isbn-2", item_score=20),
            # Item(item_id="movie-3", item_score=12),
        ]  # -> List[Item]

        # 3. Ranking items and retrieve nearest offer
        output_list = []
        for item in predicted_items:
            recommendable_items[item.item_id]["score"] = item.item_score
            recommendable_items[item.item_id]["nearest_offer_id"] = get_nearest_offer(
                db, self.user, item
            )[0].offer_id
            output_list.append(recommendable_items[item.item_id])

        # 4. TODO : Add maximum distance filter
        # 5. TODO : Add order query to sort recommended offers as wanted          order_query=sql.SQL(f"ORDER BY {order_query}"),
        # 6. TODO : Add recommended offers limit             offer_limit=sql.SQL(f"LIMIT {offer_limit}"),

        return output_list

    def save_recommendation(self, db: Session, recommendations) -> None:
        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            for reco in recommendations:
                reco_offer = PastRecommendedOffers(
                    call_id=self.user.call_id,
                    userid=self.user.user_id,
                    offerid=reco["nearest_offer_id"],
                    date=date,
                    group_id="group_id",  # temp
                    model_name="model_name",  # temp
                    model_version="model_version",  # temp
                )
                db.add(reco_offer)
            db.commit()
            # log_duration(f"save_recommendations for {self.user.id}", start)
