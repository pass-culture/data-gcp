from pcreco.core.user import User
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import (
    RecommendableItemQueryBuilder,
    RecommendableOfferQueryBuilder,
    RecommendableIrisOffersQueryBuilder,
)
from pcreco.utils.db.db_connection import get_session
from pcreco.utils.env_vars import log_duration
from typing import List, Dict, Any
import time
import random
from pcreco.core.scorer import ModelEndpoint
from pcreco.utils.env_vars import (
    log_duration,
)


class ScorerRetrieval:
    def __init__(
        self,
        user: User,
        params_in: PlaylistParamsIn,
        model_endpoint: ModelEndpoint,
        model_params,
    ):
        self.user = user
        self.model_params = model_params
        self.params_in_filters = params_in._get_conditions()
        self.model_endpoint = model_endpoint


class OfferIrisRetrieval(ScorerRetrieval):
    """
    Takes top k {retrieval_limit} offers around user (iris_based) and returns top n {ranking_limit} most similar (model based).
    """

    def get_scoring(self) -> List[str]:
        start = time.time()
        # Retrieval
        recommendable_offers = self.get_recommendable_offers()
        log_duration(
            f"Retrieval: get_recommendable_items for {self.user.id}: items -> {len(recommendable_offers)}",
            start,
        )
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
        start = time.time()
        recommendable_offers_query = RecommendableIrisOffersQueryBuilder(
            self.params_in_filters, self.model_params.retrieval_limit
        ).generate_query(
            order_query=self.model_params.retrieval_order_query,
            user=self.user,
        )

        query_result = []
        if recommendable_offers_query is not None:
            connection = get_session()
            query_result = connection.execute(recommendable_offers_query).fetchall()

        user_recommendation = {
            row[1]: {
                "id": row[0],
                "item_id": row[1],
                "venue_id": row[2],
                "user_distance": row[3],
                "booking_number": row[4],
                "category": row[5],
                "subcategory_id": row[6],
                "search_group_name": row[7],
                "is_geolocated": row[8],
                "random": random.random(),
            }
            for row in query_result
        }
        log_duration(f"get_recommendable_offers for {self.user.id}", start)
        return user_recommendation


class OfferIrisScorer(OfferIrisRetrieval):
    def get_scoring(self) -> List[str]:
        start = time.time()
        # Retrieval
        recommendable_offers = self.get_recommendable_offers()
        log_duration(
            f"Retrieval: get_recommendable_items for {self.user.id}: items -> {len(recommendable_offers)}",
            start,
        )
        return recommendable_offers.values()


class ItemRetrievalRanker(ScorerRetrieval):
    """
    Takes top k {retrieval_limit} items, get most 500 similar ones (model_based) and returns top n {ranking_limit} nearest offers and similar offers
    """

    def get_scoring(self) -> List[str]:
        start = time.time()
        # Retrieval Phase
        recommendable_items = self.get_recommendable_items()
        log_duration(
            f"Retrieval: get_recommendable_items for {self.user.id}: items -> {len(recommendable_items)}",
            start,
        )
        start = time.time()
        # nothing to score
        if len(recommendable_items) == 0:
            return []

        prediction_result = self.model_endpoint.model_score(
            recommendable_items, size=500
        )
        log_duration(
            f"Retrieval: predicted_items for {self.user.id}: predicted_items -> {len(prediction_result)}",
            start,
        )
        start = time.time()
        # nothing to score
        if len(prediction_result) == 0:
            return []

        # Ranking Phase
        recommendable_offers = self.get_recommendable_offers(prediction_result)
        log_duration(
            f"Ranking: get_recommendable_offers for {self.user.id}: offers -> {len(recommendable_offers)}",
            start,
        )

        return recommendable_offers.values()

    def get_recommendable_offers(
        self, selected_items_list
    ) -> Dict[str, Dict[str, Any]]:
        start = time.time()
        recommendable_offers_query = RecommendableOfferQueryBuilder(
            self.params_in_filters
        ).generate_query(
            order_query=self.model_params.ranking_order_query,
            offer_limit=self.model_params.ranking_limit,
            selected_items=selected_items_list,
            user=self.user,
        )

        query_result = []
        if recommendable_offers_query is not None:
            connection = get_session()
            query_result = connection.execute(recommendable_offers_query).fetchall()

        user_recommendation = {
            row[1]: {
                "id": row[0],
                "item_id": row[1],
                "venue_id": row[2],
                "user_distance": row[3],
                "booking_number": row[4],
                "category": row[5],
                "subcategory_id": row[6],
                "search_group_name": row[7],
                "venue_latitude": row[8],
                "venue_longitude": row[9],
                "item_score": row[10],
                "order": i - self.model_params.ranking_limit,
                "random": random.random(),
            }
            for i, row in enumerate(query_result)
        }
        log_duration(
            f"get_recommendable_offers for {self.user.id}: offers -> {len(user_recommendation)}",
            start,
        )
        return user_recommendation

    def get_recommendable_items(self) -> List[str]:

        start = time.time()
        recommendable_offers_query = RecommendableItemQueryBuilder(
            self.params_in_filters
        ).generate_query(
            order_query=self.model_params.retrieval_order_query,
            offer_limit=self.model_params.retrieval_limit,
            user=self.user,
        )

        query_result = []
        if recommendable_offers_query is not None:
            connection = get_session()
            query_result = connection.execute(recommendable_offers_query).fetchall()

        user_recommendation = [row[0] for row in query_result]
        log_duration(
            f"get_recommendable_items for {self.user.id} items -> {len(user_recommendation)}",
            start,
        )
        return user_recommendation
