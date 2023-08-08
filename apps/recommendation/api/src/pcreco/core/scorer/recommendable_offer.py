from pcreco.core.user import User
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import (
    RecommendableOfferQueryBuilder,
)
from pcreco.utils.db.db_connection import get_session
from pcreco.utils.env_vars import log_duration
from typing import List
import time
import random
from pcreco.core.scorer.retrieval_endpoint import RetrievalEndpoint
from pcreco.core.scorer.ranking_endpoint import RankingEndpoint
from dataclasses import dataclass


@dataclass
class RecommendableOffer:
    offer_id: str
    item_id: str
    venue_id: str
    user_distance: float
    booking_number: float
    category: str
    subcategory_id: str
    stock_price: float
    offer_creation_date: str
    stock_beginning_date: str
    search_group_name: str
    venue_latitude: float
    venue_longitude: float
    item_score: float  # lower = better
    order: int
    random: float
    offer_score: float = None  # higher = better


class ScorerRetrieval:
    def __init__(
        self,
        user: User,
        params_in: PlaylistParamsIn,
        retrieval_endpoint: RetrievalEndpoint,
        ranking_endpoint: RankingEndpoint,
        model_params,
    ):
        self.user = user
        self.model_params = model_params
        self.params_in = params_in
        self.retrieval_endpoint = retrieval_endpoint
        self.ranking_endpoint = ranking_endpoint

    def get_scoring(self) -> List[RecommendableOffer]:
        start = time.time()
        prediction_items = self.retrieval_endpoint.model_score(
            size=self.model_params.retrieval_limit
        )
        log_duration(
            f"Retrieval: predicted_items for {self.user.id}: predicted_items -> {len(prediction_items)}",
            start,
        )
        start = time.time()
        # nothing to score
        if len(prediction_items) == 0:
            return []

        # Ranking Phase
        recommendable_offers = self.get_recommendable_offers(prediction_items)

        recommendable_offers = self.ranking_endpoint.model_score(
            recommendable_offers=recommendable_offers
        )
        log_duration(
            f"Ranking: get_recommendable_offers for {self.user.id}: offers -> {len(recommendable_offers)}",
            start,
        )
        return recommendable_offers

    def get_recommendable_offers(self, selected_items_list) -> List[RecommendableOffer]:
        start = time.time()
        recommendable_offers_query = RecommendableOfferQueryBuilder().generate_query(
            order_query=self.model_params.ranking_order_query,
            offer_limit=self.model_params.ranking_limit,
            selected_items=selected_items_list,
            user=self.user,
        )

        query_result = []
        if recommendable_offers_query is not None:
            connection = get_session()
            query_result = connection.execute(recommendable_offers_query).fetchall()

        user_recommendation = [
            RecommendableOffer(
                offer_id=row[0],
                item_id=row[1],
                venue_id=row[2],
                user_distance=row[3],
                booking_number=row[4],
                stock_price=row[5],
                offer_creation_date=row[6],
                stock_beginning_date=row[7],
                category=row[8],
                subcategory_id=row[9],
                search_group_name=row[10],
                venue_latitude=row[11],
                venue_longitude=row[12],
                item_score=row[13],
                order=i,
                random=random.random(),
                offer_score=i,  # TODO
            )
            for i, row in enumerate(query_result)
        ]
        log_duration(
            f"get_recommendable_offers for {self.user.id}: offers -> {len(user_recommendation)}",
            start,
        )
        return user_recommendation
