from schemas.user import User
from schemas.playlist_params import PlaylistParamsRequest
from core.utils.query_builder import (
    RecommendableIrisOffersQueryBuilder,
)
# from core.utils.db.db_connection import get_session
# from pcreco.utils.env_vars import log_duration
from typing import List, Dict, Any
import time
import random
from core.scorer import ModelEndpoint
from sqlalchemy.orm import Session
from utils.database import Base, engine

class ScorerRetrieval:
    def __init__(
        self,
        user: User,
        params_in: PlaylistParamsRequest,
        model_endpoint: ModelEndpoint,
        model_params,
    ):
        self.user = user
        self.model_params = model_params
        self.params_in_filters = params_in._get_conditions()
        self.params_in = params_in
        self.model_endpoint = model_endpoint


class OfferIrisRetrieval(ScorerRetrieval):
    """
    Takes top k {retrieval_limit} offers around user (iris_based) and returns top n {ranking_limit} most similar (model based).
    """

    def get_scoring(self) -> List[str]:
        start = time.time()
        # Retrieval
        recommendable_offers = self.get_recommendable_offers()
        # log_duration(
        #     f"Retrieval: get_recommendable_items for {self.user.id}: items -> {len(recommendable_offers)}",
        #     start,
        # )
        # nothing to score
        if len(recommendable_offers) == 0:
            return []
        selected_offers = list(recommendable_offers.keys())
        # Ranking
        predicted_offers = self.model_endpoint.model_score(
            selected_offers, size=self.model_params.ranking_limit
        )
        # Ranking
        output_list = []
        for item_id, score in predicted_offers.items():
            recommendable_offers[item_id]["score"] = score
            output_list.append(recommendable_offers[item_id])

        return output_list

    def get_recommendable_offers(self) -> Dict[str, Dict[str, Any]]:
        #     start = time.time()
        # recommendable_offers_query = RecommendableIrisOffersQueryBuilder(
        #     self.params_in_filters, self.model_params.retrieval_limit
        # ).generate_query(
        #     order_query=self.model_params.retrieval_order_query,
        #     user=self.user,
        # )

        # query_result = []
        # if recommendable_offers_query is not None:
        #     connection = Session(bind=engine, expire_on_commit=False)
        #     query_result = connection.execute(recommendable_offers_query).fetchall()

        user_recommendation = {
            # FOR TEST ONLY
            "isbn-1": {
                "id": 1,
                "item_id": 'isbn-1',
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
                "item_id": 'movie-3',
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
        # log_duration(f"get_recommendable_offers for {self.user.id}", start)
        return user_recommendation





