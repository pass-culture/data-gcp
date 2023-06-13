from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import (
    RecommendableItemQueryBuilder,
    RecommendableOfferQueryBuilder,
    RecommendableIrisOffersQueryBuilder,
)
from pcreco.utils.db.db_connection import get_session
from pcreco.core.utils.vertex_ai import endpoint_score
from pcreco.utils.env_vars import log_duration
from typing import List, Dict, Any
import time
import random


class SimilarOfferScorer:
    def __init__(
        self, user: User, offer: Offer, params_in: PlaylistParamsIn, model_params
    ):
        self.user = user
        self.offer = offer
        self.model_params = model_params
        self.params_in_filters = params_in._get_conditions()
        self.reco_origin = "default"
        self.model_version = None
        self.model_display_name = None

    def similar_offer_model_score(self, selected_items, size):
        start = time.time()
        instances = {
            "offer_id": self.offer.item_id,
            "selected_offers": selected_items,
            "size": size,
        }
        prediction_result = endpoint_score(
            instances=instances, endpoint_name=self.model_params.endpoint_name
        )
        self.model_version = prediction_result.model_version
        self.model_display_name = prediction_result.model_display_name
        log_duration("similar_offer_model_score", start)
        return prediction_result.predictions


class OfferIrisRetrieval(SimilarOfferScorer):
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
        predicted_offers = self.similar_offer_model_score(
            selected_offers, size=self.model_params.ranking_limit
        )
        return [recommendable_offers[offer]["id"] for offer in predicted_offers]

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
            }
            for row in query_result
            if row[1] != self.offer.item_id
        }
        log_duration(f"get_recommendable_offers for {self.user.id}", start)
        return user_recommendation


class OfferIrisRandomScorer(OfferIrisRetrieval):
    """
    Takes top k {retrieval_limit} offers around user (iris_based) and returns random n {ranking_limit}.
    """

    def get_scoring(self) -> List[str]:
        start = time.time()
        # Retrieval
        recommendable_offers = self.get_recommendable_offers()
        log_duration(
            f"Retrieval: get_recommendable_items for {self.user.id}: items -> {len(recommendable_offers)}",
            start,
        )
        size_recommendable_offers = len(recommendable_offers)
        # nothing to score
        if size_recommendable_offers == 0:
            return []

        selected_offers = list(recommendable_offers.keys())
        # Ranking
        predicted_offers = random.sample(
            selected_offers,
            k=min(self.model_params.ranking_limit, size_recommendable_offers),
        )
        return [recommendable_offers[offer]["id"] for offer in predicted_offers]


class ItemRetrievalRanker(SimilarOfferScorer):
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
        # nothing to score
        if len(recommendable_items) == 0:
            return []

        prediction_result = self.similar_offer_model_score(
            list(recommendable_items), size=500
        )
        log_duration(
            f"Retrieval: predicted_items for {self.user.id}: predicted_items -> {len(prediction_result)}",
            start,
        )
        # nothing to score
        if len(prediction_result) == 0:
            return []

        # Ranking Phase
        recommendable_offers = self.get_recommendable_offers(prediction_result)
        log_duration(
            f"Ranking: get_recommendable_offers for {self.user.id}: offers -> {len(recommendable_offers)}",
            start,
        )
        return [recommendable_offers[offer]["id"] for offer in recommendable_offers]

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
                "item_rank": row[10],
            }
            for row in query_result
            if row[1] != self.offer.item_id
        }
        log_duration(
            f"get_recommendable_offers for {self.user.id}: offers -> {len(user_recommendation)}",
            start,
        )
        return user_recommendation

    def get_recommendable_items(self) -> Dict[str, Dict[str, Any]]:

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

        user_recommendation = [
            row[0] for row in query_result if row[0] != self.offer.item_id
        ]
        log_duration(
            f"get_recommendable_items for {self.user.id} items -> {len(user_recommendation)}",
            start,
        )
        return user_recommendation
