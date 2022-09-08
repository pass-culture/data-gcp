# pylint: disable=invalid-name
import json
import random
from sqlalchemy import text
from pcreco.core.user import User
from pcreco.core.utils.cold_start_status import get_cold_start_status
from pcreco.core.utils.diversification import (
    order_offers_by_score_and_diversify_categories,
)
from pcreco.utils.ai_platform.service import ai_platform_service

from pcreco.models.reco.recommendation import RecommendationIn
from pcreco.utils.db.db_connection import get_db
from pcreco.core.utils.vertex_ai import predict_custom_trained_model_sample
from pcreco.utils.env_vars import (
    GCP_PROJECT,
    PROJECT_NUMBER,
    MACRO_CATEGORIES_TYPE_MAPPING,
    NUMBER_OF_PRESELECTED_OFFERS,
    ACTIVE_MODEL,
    ENDPOINT_ID,
    AB_TESTING,
    AB_TEST_MODEL_DICT,
    RECOMMENDABLE_OFFER_LIMIT,
    SHUFFLE_RECOMMENDATION,
    log_duration,
)
import datetime
import time
import pytz
from typing import List, Dict, Any


class Scoring:
    def __init__(self, user: User, recommendation_in: RecommendationIn = None):
        self.user = user
        self.json_input = recommendation_in.json_input if recommendation_in else None
        self.recommendation_in_filters = (
            recommendation_in._get_conditions() if recommendation_in else ""
        )
        self.recommendation_in_model_name = (
            recommendation_in.model_name if recommendation_in else None
        )
        self.iscoldstart = (
            False if self.force_model else get_cold_start_status(self.user)
        )
        self.model_name = self.get_model_name()
        self.scoring = self.get_scoring_method()

    # rename force model
    @property
    def force_model(self) -> bool:
        if self.recommendation_in_model_name:
            return True
        else:
            return False

    def get_model_name(self) -> str:
        if self.force_model:
            return self.recommendation_in_model_name
        elif AB_TESTING:
            return AB_TEST_MODEL_DICT[f"{self.user.group_id}"]
        else:
            return ACTIVE_MODEL

    def get_scoring_method(self) -> object:
        if self.iscoldstart:
            scoring_method = self.ColdStart(self)
        else:
            scoring_method = self.Algo(self)
        return scoring_method

    def get_recommendation(self) -> List[str]:
        # score the offers
        final_recommendations = order_offers_by_score_and_diversify_categories(
            sorted(
                self.scoring.get_scored_offers(), key=lambda k: k["score"], reverse=True
            )[:NUMBER_OF_PRESELECTED_OFFERS],
            SHUFFLE_RECOMMENDATION,
        )

        return final_recommendations

    def save_recommendation(self, recommendations) -> None:
        if len(recommendations) > 0:
            start = time.time()
            date = datetime.datetime.now(pytz.utc)
            rows = []

            for offer_id in recommendations:
                rows.append(
                    {
                        "user_id": self.user.id,
                        "offer_id": offer_id,
                        "date": date,
                        "group_id": self.user.group_id,
                        "reco_origin": "cold-start" if self.iscoldstart else "algo",
                        "model_name": self.scoring.model_display_name,
                        "model_version": self.scoring.model_version,
                        "reco_filters": json.dumps(self.json_input),
                        "call_id": self.user.call_id,
                        "lat": self.user.latitude,
                        "long": self.user.longitude,
                    }
                )

            connection = get_db()
            connection.execute(
                text(
                    """
                    INSERT INTO public.past_recommended_offers (userid, offerid, date, group_id, reco_origin, model_name, model_version, reco_filters, call_id, lat, long)
                    VALUES (:user_id, :offer_id, :date, :group_id, :reco_origin, :model_name, :model_version, :reco_filters, :call_id, :lat, :long)
                    """
                ),
                rows,
            )
            log_duration(f"save_recommendations for {self.user.id}", start)

    class Algo:
        def __init__(self, scoring):
            self.user = scoring.user
            self.recommendation_in_filters = scoring.recommendation_in_filters
            self.model_name = scoring.model_name
            self.model_display_name = None
            self.model_version = None
            self.recommendable_offers = self.get_recommendable_offers()

        def get_scored_offers(self) -> List[Dict[str, Any]]:
            start = time.time()
            if not len(self.recommendable_offers) > 0:
                log_duration(
                    f"no offers to score for {self.user.id} - {self.model_name}",
                    start,
                )
                return []
            else:
                instances = self._get_instances()

                predicted_scores = self._predict_score(instances)

                recommendations = [
                    {**recommendation, "score": predicted_scores[i][0]}
                    for i, recommendation in enumerate(self.recommendable_offers)
                ]

                log_duration(
                    f"scored {len(recommendations)} for {self.user.id} - {self.model_name}, ",
                    start,
                )
            return recommendations

        def _get_instances(self) -> List[Dict[str, str]]:
            user_to_rank = [self.user.id] * len(self.recommendable_offers)
            offer_ids_to_rank = []
            for recommendation in self.recommendable_offers:
                offer_ids_to_rank.append(
                    recommendation["item_id"] if recommendation["item_id"] else ""
                )
            instances = {"input_1": user_to_rank, "input_2": offer_ids_to_rank}
            return instances

        def get_recommendable_offers(self) -> List[Dict[str, Any]]:
            query = text(self._get_intermediate_query())
            connection = get_db()
            query_result = connection.execute(
                query,
                user_id=str(self.user.id),
                user_iris_id=str(self.user.iris_id),
            ).fetchall()

            user_recommendation = [
                {
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
            ]

            return user_recommendation

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
                {self.recommendation_in_filters}
                ORDER BY is_numerical ASC,booking_number DESC
                LIMIT {RECOMMENDABLE_OFFER_LIMIT}; 
                """
            return query

        def _predict_score(self, instances) -> List[List[float]]:
            start = time.time()
            """Calls Vertex AI endpoint for the given model and instances and retrieves the scores."""
            response = predict_custom_trained_model_sample(
                project=PROJECT_NUMBER,
                endpoint_id=ENDPOINT_ID,
                location="europe-west1",
                instances=instances,
            )
            self.model_version = response["model_version_id"]
            self.model_display_name = response["model_display_name"]
            log_duration("predict_score", start)
            return response["predictions"]

    class ColdStart:
        def __init__(self, scoring):
            self.user = scoring.user
            self.recommendation_in_filters = scoring.recommendation_in_filters
            self.cold_start_categories = self.get_cold_start_categories()
            self.model_version = None
            self.model_display_name = None

        def get_scored_offers(self) -> List[Dict[str, Any]]:
            order_query = (
                f"""ORDER BY (category in ({', '.join([f"'{category}'" for category in self.cold_start_categories])})) DESC, booking_number DESC"""
                if self.cold_start_categories
                else "ORDER BY booking_number DESC"
            )

            where_clause = (
                f"""(venue_id IN (SELECT "venue_id" FROM iris_venues_mv WHERE "iris_id" = :user_iris_id) OR is_national = True OR url IS NOT NULL)"""
                if self.user.iris_id
                else "(is_national = True or url IS NOT NULL)"
            )
            recommendations_query = text(
                f"""
                SELECT offer_id, category,subcategory_id,search_group_name, url, product_id
                FROM {self.user.recommendable_offer_table}
                WHERE offer_id NOT IN
                    (
                        SELECT offer_id
                        FROM non_recommendable_offers
                        WHERE user_id = :user_id
                    )
                {self.recommendation_in_filters}
                AND {where_clause}
                {order_query}
                LIMIT :number_of_preselected_offers;
                """
            )
            connection = get_db()
            query_result = connection.execute(
                recommendations_query,
                user_iris_id=str(self.user.iris_id),
                user_id=str(self.user.id),
                number_of_preselected_offers=NUMBER_OF_PRESELECTED_OFFERS,
            ).fetchall()

            cold_start_recommendations = [
                {
                    "id": row[0],
                    "category": row[1],
                    "subcategory_id": row[2],
                    "search_group_name": row[3],
                    "url": row[4],
                    "product_id": row[5],
                    "score": random.random(),
                }
                for row in query_result
            ]

            return cold_start_recommendations

        def get_cold_start_categories(self) -> List[str]:
            cold_start_query = text(
                f"""SELECT subcategories FROM qpi_answers_mv WHERE user_id = :user_id;"""
            )

            connection = get_db()
            query_result = connection.execute(
                cold_start_query,
                user_id=str(self.user.id),
            ).fetchall()

            if len(query_result) == 0:
                return []
            cold_start_categories = [res[0] for res in query_result]
            return cold_start_categories
