# pylint: disable=invalid-name
from sqlalchemy import text
from pcreco.core.user import User
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import RecommendableOffersQueryBuilder
from pcreco.utils.db.db_connection import get_session
from pcreco.core.utils.vertex_ai import predict_model
from pcreco.utils.env_vars import (
    SIM_OFFERS_ENDPOINT_NAME,
    RECOMMENDABLE_OFFER_LIMIT,
    DEFAULT_RECO_RADIUS,
    log_duration,
)
from typing import List, Dict, Any
import time
from loguru import logger


class SimilarOffer:
    def __init__(self, user: User, offer_id: int, params_in: PlaylistParamsIn = None):
        self.user = user
        self.n = 10
        self.recommendable_offer_limit = 5000
        self.params_in_filters = params_in._get_conditions() if params_in else ""
        self.reco_radius = params_in.reco_radius if params_in else DEFAULT_RECO_RADIUS
        self.include_numericals = True
        # TODO move this in exec
        self.item_id = self.get_item_id(offer_id)
        self.recommendable_offers = self.get_recommendable_offers()

        self.model_version = None
        self.model_display_name = None

    def get_scored_offers(self) -> List[Dict[str, Any]]:
        instances = self._get_instances()
        if instances is None:
            return []
        predicted_offers = self._predict_score(instances)
        recommendations = []
        for offer in predicted_offers:
            recommendations.append(self.recommendable_offers[offer])
        return recommendations

    def get_scoring(self) -> List[Dict[str, Any]]:
        offers = self.get_scored_offers()
        return [offer["id"] for offer in offers]

    def _get_instances(self) -> List[Dict[str, str]]:
        if self.item_id is None:
            logger.info(f"_get_instances:offer_not_found")
            return None
        if len(self.recommendable_offers) > 0:
            selected_offers = list(self.recommendable_offers.keys())
            logger.info(f"_get_instances:{','.join(selected_offers)}")
            return {
                "offer_id": self.item_id,
                "selected_offers": selected_offers,
                "size": self.n,
            }
        else:
            logger.info(f"_get_instances:all")
            return {"offer_id": self.item_id, "n": self.n}

    def get_recommendable_offers(self) -> Dict[str, Dict[str, Any]]:
        start = time.time()
        order_query = "is_geolocated DESC, booking_number DESC"
        query = text(
            RecommendableOffersQueryBuilder(
                self, RECOMMENDABLE_OFFER_LIMIT
            ).generate_query(order_query)
        )
        connection = get_session()
        query_result = connection.execute(
            query,
            user_id=str(self.user.id),
            user_iris_id=str(self.user.iris_id),
            user_longitude=float(self.user.longitude),
            user_latitude=float(self.user.latitude),
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
        }
        logger.info(f"get_recommendable_offers: n: {len(user_recommendation)}")
        log_duration(f"get_recommendable_offers for {self.user.id}", start)
        return user_recommendation

    def get_item_id(self, offer_id) -> str:
        start = time.time()
        connection = get_session()
        query_result = connection.execute(
            f"""
                SELECT item_id 
                FROM item_ids_mv
                WHERE offer_id = '{offer_id}'
            """
        ).fetchone()
        log_duration(f"get_item_id for offer_id: {offer_id}", start)
        if query_result is not None:
            logger.info("get_item_id:found id")
            return query_result[0]
        else:
            logger.info("get_item_id:not_found_id")
            return None

    def _predict_score(self, instances) -> List[List[float]]:
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
