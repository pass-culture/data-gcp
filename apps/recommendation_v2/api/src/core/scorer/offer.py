from schemas.user import User
from schemas.playlist_params import PlaylistParams
from typing import List
import time
from core.endpoint.retrieval_endpoint import RetrievalEndpoint

# from endpoint.ranking_endpoint import RankingEndpoint
from schemas.offer import RecommendableOffer
from schemas.item import RecommendableItem
from crud.offer import get_nearest_offer, get_non_recommendable_items
from sqlalchemy.orm import Session
from utils.manage_output_offers import limit_offers


class OfferScorer:
    def __init__(
        self,
        user: User,
        params_in: PlaylistParams,
        retrieval_endpoint: RetrievalEndpoint,
        # ranking_endpoint: RankingEndpoint,
        model_params,
    ):
        self.user = user
        self.model_params = model_params
        self.params_in = params_in
        self.retrieval_endpoint = retrieval_endpoint
        # self.ranking_endpoint = ranking_endpoint

    def get_scoring(
        self,
        db: Session,
        offer_limit: int = 20,
    ) -> List[RecommendableOffer]:
        start = time.time()
        # 1. Score items
        prediction_items = self.retrieval_endpoint.model_score(
            size=self.model_params.retrieval_limit
        )
        print(
            f"OfferScorer - get_scoring - prediction_items: {prediction_items}"
        )  # Example output : RecommendableItem(item_id='product-57171', item_score=0.1650311350822449)
        # log_duration(
        #     f"Retrieval: predicted_items for {self.user.id}: predicted_items -> {len(prediction_items)}",
        #     start,
        # )
        start = time.time()
        # nothing to score
        if len(prediction_items) == 0:
            return []

        # Transform items in offers
        recommendable_offers = self.get_recommendable_offers(db, prediction_items)

        # nothing to score
        if len(recommendable_offers) == 0:
            return []

        # recommendable_offers = self.ranking_endpoint.model_score(
        #     recommendable_offers=recommendable_offers
        # )
        # log_duration(
        #     f"Ranking: get_recommendable_offers for {self.user.id}: offers -> {len(recommendable_offers)}",
        #     start,
        # )

        # Limit the display of offers recommendations
        user_recommendations = limit_offers(
            offer_limit=offer_limit, list_offers=recommendable_offers
        )

        print(f"Nb recommendations : {len(user_recommendations)}")

        return user_recommendations

    def get_recommendable_offers(
        self,
        db: Session,
        recommendable_items: List[RecommendableItem],
    ) -> List[RecommendableOffer]:

        print(f"Nb recommendable items : {len(recommendable_items)}")

        non_recommendable_items = get_non_recommendable_items(db, self.user)
        print(f"Nb non recommendable items : {len(non_recommendable_items)}")

        recommendable_offers = []
        for item in recommendable_items:
            if item.item_id not in non_recommendable_items:
                recommendable_offer = get_nearest_offer(db, self.user, item)
                if recommendable_offer:
                    recommendable_offers.append(recommendable_offer[0])

        print(f"Nb recommendable offers : {len(recommendable_offers)}")

        return recommendable_offers
