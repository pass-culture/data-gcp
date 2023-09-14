from sqlalchemy.orm import Session
from typing import List
import time
import random

from huggy.core.endpoint.retrieval_endpoint import RetrievalEndpoint
from huggy.core.endpoint.ranking_endpoint import RankingEndpoint

from huggy.schemas.user import User
from huggy.schemas.playlist_params import PlaylistParams
from huggy.schemas.offer import RecommendableOffer
from huggy.schemas.item import RecommendableItem

from huggy.crud.offer import get_nearest_offers

from huggy.utils.manage_output_offers import limit_offers
from huggy.utils.env_vars import log_duration


class OfferScorer:
    def __init__(
        self,
        user: User,
        params_in: PlaylistParams,
        retrieval_endpoints: List[RetrievalEndpoint],
        ranking_endpoint: RankingEndpoint,
        model_params,
    ):
        self.user = user
        self.model_params = model_params
        self.params_in = params_in
        self.retrieval_endpoints = retrieval_endpoints
        self.ranking_endpoint = ranking_endpoint

    def get_scoring(
        self,
        db: Session,
        offer_limit: int = 20,
    ) -> List[RecommendableOffer]:
        start = time.time()

        prediction_items: List[RecommendableItem] = []

        for endpoint in self.retrieval_endpoints:
            prediction_items.extend(endpoint.model_score())
        log_duration(
            f"Retrieval: predicted_items for {self.user.user_id}: predicted_items -> {len(prediction_items)}",
            start,
        )
        start = time.time()
        # nothing to score
        if len(prediction_items) == 0:
            return []

        # Transform items in offers
        recommendable_offers = self.get_recommendable_offers(db, prediction_items)

        # nothing to score
        if len(recommendable_offers) == 0:
            return []

        recommendable_offers = self.ranking_endpoint.model_score(
            recommendable_offers=recommendable_offers
        )
        log_duration(
            f"Ranking: get_recommendable_offers for {self.user.user_id}: offers -> {len(recommendable_offers)}",
            start,
        )

        # Limit the display of offers recommendations
        user_recommendations = limit_offers(
            offer_limit=offer_limit, list_offers=recommendable_offers
        )

        return user_recommendations

    def get_recommendable_offers(
        self,
        db: Session,
        recommendable_items: List[RecommendableItem],
    ) -> List[RecommendableOffer]:

        recommendable_offers = get_nearest_offers(db, self.user, recommendable_items)

        size = len(recommendable_offers)

        for i, recommendable_offer in enumerate(recommendable_offers):
            recommendable_offer.offer_score = size - i
            recommendable_offer.query_order = i
            recommendable_offer.random = random.random()

        return recommendable_offers
