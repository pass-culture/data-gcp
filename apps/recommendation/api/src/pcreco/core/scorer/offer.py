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
from pcreco.core.endpoint.retrieval_endpoint import RetrievalEndpoint
from pcreco.core.endpoint.ranking_endpoint import RankingEndpoint
from pcreco.core.model.recommendable_offer import RecommendableOffer
from pcreco.core.model.recommendable_item import RecommendableItem


class OfferScorer:
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

        # nothing to score
        if len(recommendable_offers) == 0:
            return []

        recommendable_offers = self.ranking_endpoint.model_score(
            recommendable_offers=recommendable_offers
        )
        log_duration(
            f"Ranking: get_recommendable_offers for {self.user.id}: offers -> {len(recommendable_offers)}",
            start,
        )
        return recommendable_offers

    def get_recommendable_offers(
        self, recommendable_items: List[RecommendableItem]
    ) -> List[RecommendableOffer]:
        start = time.time()
        recommendable_offers_query = RecommendableOfferQueryBuilder().generate_query(
            order_query=self.model_params.ranking_order_query,
            offer_limit=self.model_params.ranking_limit,
            recommendable_items=recommendable_items,
            user=self.user,
        )

        query_result = []
        if recommendable_offers_query is not None:
            connection = get_session()
            query_result = connection.execute(recommendable_offers_query).fetchall()

        size = len(query_result)

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
                item_score=row[13],  # lower is better
                query_order=i,
                random=random.random(),
                offer_score=size - i,  # higher is better
            )
            for i, row in enumerate(query_result)
        ]
        log_duration(
            f"get_recommendable_offers for {self.user.id}: offers -> {len(user_recommendation)}",
            start,
        )
        return user_recommendation
