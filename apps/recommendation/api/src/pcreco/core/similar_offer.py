# pylint: disable=invalid-name
from sqlalchemy import text
import json
from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import RecommendableIrisOffersQueryBuilder
from pcreco.utils.db.db_connection import get_session
from pcreco.core.utils.vertex_ai import predict_model
from pcreco.core.model_selection import select_sim_model_params
from pcreco.utils.env_vars import log_duration, ENV_SHORT_NAME
from typing import List, Dict, Any
from loguru import logger
import datetime
import time
import pytz
import random


class SimilarOffer:
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParamsIn):
        self.user = user
        self.offer = offer
        self.n = 20
        self.model_params = select_sim_model_params(params_in.model_endpoint)
        self.recommendable_offer_limit = 10_000
        self.params_in_filters = params_in._get_conditions()
        self.json_input = params_in.json_input
        self.include_digital = params_in.include_digital
        self.has_conditions = params_in.has_conditions
        self.reco_origin = "default"
        self.model_version = None
        self.model_display_name = None

    def get_scoring(self) -> List[str]:

        # item not found
        if self.offer.item_id is None:
            return []
        # get recommendable_offers
        recommendable_offers = self.get_recommendable_offers()
        size_recommendable_offers = len(recommendable_offers)
        # nothing to score
        if size_recommendable_offers == 0:
            return []

        selected_offers = list(recommendable_offers.keys())
        # TODO: fix waiting to have enough offers in dev.
        if ENV_SHORT_NAME == "dev":
            predicted_offers = random.sample(
                selected_offers, k=min(self.n, size_recommendable_offers)
            )
            return [recommendable_offers[offer]["id"] for offer in predicted_offers]

        instances = {
            "offer_id": self.offer.item_id,
            "selected_offers": selected_offers,
            "size": self.n,
        }
        predicted_offers = self._predict_score(instances)
        return [recommendable_offers[offer]["id"] for offer in predicted_offers]

    def get_recommendable_offers(self) -> Dict[str, Dict[str, Any]]:

        start = time.time()
        order_query = "booking_number DESC"

        recommendable_offers_query = RecommendableIrisOffersQueryBuilder(
            self, self.recommendable_offer_limit
        ).generate_query(
            order_query,
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
            if row[4] != self.offer.item_id
        }
        logger.info(f"get_recommendable_offers: n: {len(user_recommendation)}")
        log_duration(f"get_recommendable_offers for {self.user.id}", start)
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

    def _predict_score(self, instances) -> List[List[str]]:

        logger.info(f"_predict_score: {instances}")
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
