# pylint: disable=invalid-name
from sqlalchemy import text
from pcreco.core.user import User
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.utils.db.db_connection import get_db
from pcreco.core.utils.vertex_ai import predict_model
from pcreco.utils.env_vars import (
    SIM_OFFERS_ENDPOINT_NAME,
)
from typing import List, Dict, Any
from loguru import logger


class SimilarOffer:
    def __init__(self, user: User, offer_id: int, params_in: PlaylistParamsIn = None):
        self.user = user
        self.n = 10
        self.recommendable_offer_limit = 5000
        self.params_in_filters = params_in._get_conditions() if params_in else ""
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
        query = text(self._get_intermediate_query())
        connection = get_db()
        query_result = connection.execute(
            query,
            user_id=str(self.user.id),
            user_iris_id=str(self.user.iris_id),
        ).fetchall()

        user_recommendation = {
            row[6]: {
                "id": row[0],
                "category": row[1],
                "subcategory_id": row[2],
                "search_group_name": row[3],
                "url": row[4],
                "is_numerical": row[5],
                "item_id": row[6],
                "product_id": row[7],
            }
            for row in query_result
        }
        logger.info(f"get_recommendable_offers: n: {len(user_recommendation)}")

        return user_recommendation

    def get_item_id(self, offer_id) -> str:
        connection = get_db()
        query_result = connection.execute(
            f"""
                SELECT item_id 
                FROM {self.user.recommendable_offer_table}
                WHERE offer_id = '{offer_id}'
            """
        ).fetchone()
        if query_result is not None:
            logger.info("get_item_id:found id")
            return query_result[0]
        else:
            logger.info("get_item_id:not_found_id")
            return None

    def _get_intermediate_query(self) -> str:
        geoloc_filter = (
            f"""( venue_id IN (SELECT "venue_id" FROM iris_venues_mv WHERE "iris_id" = :user_iris_id) OR is_national = True OR url IS NOT NULL)"""
            if self.user.iris_id
            else "(is_national = True or url IS NOT NULL)"
        )
        query = f"""
            SELECT offer_id, category, subcategory_id,search_group_name, url, url IS NOT NULL as is_numerical, item_id, product_id
            FROM {self.user.recommendable_offer_table}
            WHERE {geoloc_filter}
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )   
            {self.params_in_filters}
            ORDER BY is_numerical ASC,booking_number DESC
            LIMIT {self.recommendable_offer_limit}; 
            """
        return query

    def _predict_score(self, instances) -> List[List[float]]:
        logger.info(f"_predict_score: {instances}")
        response = predict_model(
            endpoint_name=SIM_OFFERS_ENDPOINT_NAME,
            location="europe-west1",
            instances=instances,
        )
        self.model_version = response["model_version_id"]
        self.model_display_name = response["model_display_name"]
        return response["predictions"]
