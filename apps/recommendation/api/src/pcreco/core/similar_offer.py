# pylint: disable=invalid-name
from sqlalchemy import text
import json
from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import (
    RecommendableItemQueryBuilder,
    RecommendableOfferQueryBuilder,
)
from pcreco.utils.db.db_connection import get_session
from pcreco.core.utils.vertex_ai import predict_model
from pcreco.core.model_selection import select_sim_model_params
from pcreco.utils.env_vars import log_duration
from typing import List, Dict, Any
import datetime
import time
import pytz


class SimilarOffer:
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParamsIn):
        self.user = user
        self.offer = offer
        self.model_params = select_sim_model_params(params_in.model_endpoint)
        self.params_in_filters = params_in._get_conditions()
        self.json_input = params_in.json_input
        self.has_conditions = params_in.has_conditions
        self.reco_origin = "default"
        self.model_version = None
        self.model_display_name = None

    def get_scoring(self) -> List[str]:
        start = time.time()

        # item not found
        if self.offer.item_id is None:
            return []

        # Retrieval Phase
        recommendable_items = self.get_recommendable_items()
        log_duration(
            f"Retrieval: get_recommendable_items for {self.user.id}: items -> {len(recommendable_items)}",
            start,
        )

        # nothing to score
        if len(recommendable_items) == 0:
            return []

        selected_items = list(recommendable_items)

        instances = {
            "offer_id": self.offer.item_id,
            "selected_offers": selected_items,
            "size": 500,
        }
        predicted_items = self.retrieval(instances)
        log_duration(
            f"Retrieval: predicted_items for {self.user.id}: predicted_items -> {len(predicted_items)}",
            start,
        )
        # nothing to score
        if len(predicted_items) == 0:
            return []

        # Ranking Phase
        recommendable_offers = self.get_recommendable_offers(predicted_items)
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
            self
        ).generate_query(
            order_query=self.model_params.ranking_order_query,
            offer_limit=self.model_params.ranking_offer_limit,
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
            self,
        ).generate_query(
            order_query=self.model_params.retrieval_order_query,
            offer_limit=self.model_params.retrieval_offer_limit,
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

    def save_recommendation(self, recommendations) -> None:

        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            rows = []

            for offer_id in recommendations:
                rows.append(
                    {
                        "user_id": self.user.id,
                        "origin_offer_id": self.offer.id,
                        "offer_id": offer_id,
                        "date": date,
                        "group_id": self.model_params.name,
                        "model_name": self.model_display_name,
                        "model_version": self.model_version,
                        "reco_filters": json.dumps(self.json_input),
                        "call_id": self.user.call_id,
                        "venue_iris_id": self.offer.iris_id,
                    }
                )

            connection = get_session()
            connection.execute(
                text(
                    """
                    INSERT INTO public.past_similar_offers (user_id, origin_offer_id, offer_id, date, group_id, model_name, model_version, reco_filters, call_id, venue_iris_id)
                    VALUES (:user_id, :origin_offer_id, :offer_id, :date, :group_id, :model_name, :model_version, :reco_filters, :call_id, :venue_iris_id)
                    """
                ),
                rows,
            )
            log_duration(f"save_recommendations for {self.user.id}", start)

    def retrieval(self, instances) -> List[str]:
        start = time.time()
        response = predict_model(
            endpoint_name=self.model_params.endpoint_name,
            location="europe-west1",
            instances=instances,
        )
        self.model_version = response["model_version_id"]
        self.model_display_name = response["model_display_name"]
        log_duration(f"_predict_score for {self.user.id}", start)
        return response["predictions"]
