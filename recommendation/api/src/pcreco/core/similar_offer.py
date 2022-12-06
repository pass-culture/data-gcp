# pylint: disable=invalid-name
from sqlalchemy import text
import json
from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import RecommendableOffersQueryBuilder
from pcreco.utils.db.db_connection import get_session
from pcreco.core.utils.vertex_ai import predict_model
from pcreco.utils.env_vars import SIM_OFFERS_ENDPOINT_NAME, log_duration, ENV_SHORT_NAME
from typing import List, Dict, Any
from loguru import logger
import datetime
import time
import pytz


class SimilarOffer:
    def __init__(self, user: User, offer: Offer, params_in: PlaylistParamsIn):
        self.user = user
        self.offer = offer
        self.n = 10
        self.recommendable_offer_limit = 10_000
        self.params_in_filters = params_in._get_conditions()
        self.reco_radius = params_in.reco_radius
        self.json_input = params_in.json_input
        self.include_numericals = params_in.include_numericals
        self.has_conditions = params_in.has_conditions

        self.model_version = None
        self.model_display_name = None

    def get_scoring(self) -> List[str]:

        # item not found
        if self.offer.item_id is None:
            return []
        # get recommendable_offers
        recommendable_offers = self.get_recommendable_offers()
        # nothing to score
        if len(recommendable_offers) == 0:
            return []

        if self.offer.cnt_bookings < 5:
            return []
        else:
            instances = {
                "offer_id": self.offer.item_id,
                "selected_offers": list(recommendable_offers.keys()),
                "size": self.n,
            }
            predicted_offers = self._predict_score(instances)
            return [recommendable_offers[offer]["id"] for offer in predicted_offers]

    def get_recommendable_offers(self) -> Dict[str, Dict[str, Any]]:

        start = time.time()
        order_query = "booking_number DESC"
        query = text(
            RecommendableOffersQueryBuilder(
                self, self.recommendable_offer_limit
            ).generate_query(order_query)
        )
        connection = get_session()
        query_result = connection.execute(
            query,
            user_id=str(self.user.id),
            user_iris_id=str(self.offer.iris_id),
            user_longitude=float(self.offer.longitude),
            user_latitude=float(self.offer.latitude),
        ).fetchall()

        user_recommendation = {
            row[4]: {
                "id": row[0],
                "category": row[1],
                "subcategory_id": row[2],
                "search_group_name": row[3],
                "item_id": row[4],
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
                        "group_id": self.user.group_id,
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
            endpoint_name=SIM_OFFERS_ENDPOINT_NAME,
            location="europe-west1",
            instances=instances,
        )
        self.model_version = response["model_version_id"]
        self.model_display_name = response["model_display_name"]
        log_duration(f"_predict_score for {self.user.id}", start)
        return response["predictions"]
