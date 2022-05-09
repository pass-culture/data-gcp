# pylint: disable=invalid-name
import random

from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from sqlalchemy import text
from refacto_api.tools.cold_start_status import get_cold_start_status
from refacto_api.tools.db_connection import create_db_connection
from refacto_api.tools.diversification import (
    order_offers_by_score_and_diversify_categories,
)
from utils import (
    ENV_SHORT_NAME,
    GCP_PROJECT,
    MACRO_CATEGORIES_TYPE_MAPPING,
    MODEL_REGION,
    NUMBER_OF_PRESELECTED_OFFERS,
    log_duration,
)
import datetime
import time
import pytz

AB_TEST_MODEL_DICT = {
    "A": f"tf_reco_{ENV_SHORT_NAME}",
    "B": f"deep_reco_{ENV_SHORT_NAME}",
    "C": f"MF_reco_{ENV_SHORT_NAME}",
}


class Scoring:
    def __init__(self, User, Playlist=None):
        self.user = User
        self.playlist_filters = Playlist._get_conditions() if Playlist else ""
        self.iscoldstart = get_cold_start_status(self.user)
        self.model_name = AB_TEST_MODEL_DICT[f"{self.user.group_id}"]
        self.scoring = self.get_scoring_method()

    def get_scoring_method(self):
        if self.iscoldstart:
            scoring_method = self.ColdStart(self)
        else:
            scoring_method = self.Algo(self)
        return scoring_method

    def get_recommendation(self):
        # score the offers
        final_recommendations = order_offers_by_score_and_diversify_categories(
            sorted(
                self.scoring.get_scored_offers(), key=lambda k: k["score"], reverse=True
            )[:NUMBER_OF_PRESELECTED_OFFERS]
        )
        return final_recommendations

    def save_recommendation(self, recommendations):
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
                }
            )

        with create_db_connection() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO public.past_recommended_offers (userid, offerid, date, group_id, reco_origin)
                    VALUES (:user_id, :offer_id, :date, :group_id, :reco_origin)
                    """
                ),
                rows,
            )
        log_duration(f"save_recommendations for {self.user.id}", start)

    class Algo:
        def __init__(self, Scoring):
            self.user = Scoring.user
            self.playlist_filters = Scoring.playlist_filters
            self.model_name = Scoring.model_name
            self.recommendable_offers = self.get_recommendable_offers()

        def get_scored_offers(self):
            start = time.time()

            instances = self._get_instances()

            predicted_scores = self._predict_score(instances)

            recommendations = [
                {**recommendation, "score": predicted_scores[i][0]}
                for i, recommendation in enumerate(self.recommendable_offers)
            ]

            log_duration(
                f"get_scored_recommendation_for_user for {self.user.id} - {self.model_name}",
                start,
            )
            return recommendations

        def _get_instances(self):
            user_to_rank = [self.user.id] * len(self.recommendable_offers)
            offer_ids_to_rank = [
                recommendation["item_id"] if recommendation["item_id"] else ""
                for recommendation in self.recommendable_offers
            ]
            offers_subcategories = [
                recommendation["subcategory_id"]
                if recommendation["subcategory_id"]
                else ""
                for recommendation in self.recommendable_offers
            ]
            instances = [{"input_1": user_to_rank, "input_2": offer_ids_to_rank}]

            if self.model_name == "deep_reco":
                instances["input_3"] = offers_subcategories
            return instances

        def get_recommendable_offers(self):
            query = text(self._get_intermediate_query())
            with create_db_connection() as connection:
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
                    "url": row[3],
                    "item_id": row[4],
                    "product_id": row[5],
                }
                for row in query_result
            ]

            return user_recommendation

        def _get_intermediate_query(self):
            geoloc_filter = (
                f"""( venue_id IN (SELECT "venue_id" FROM iris_venues_mv WHERE "iris_id" = :user_iris_id) OR is_national = True OR url IS NOT NULL)"""
                if self.user.iris_id
                else "(is_national = True or url IS NOT NULL)"
            )
            reco_booking_limit = 10 if ENV_SHORT_NAME == "prod" else 0
            query = f"""
                SELECT offer_id, category, subcategory_id, url, item_id, product_id
                FROM {self.user.recommendable_offer_table}
                WHERE {geoloc_filter}
                AND offer_id NOT IN
                    (
                    SELECT offer_id
                    FROM non_recommendable_offers
                    WHERE user_id = :user_id
                    )   
                {self.playlist_filters}
                AND booking_number >={reco_booking_limit}
                ORDER BY RANDOM(); 
                """
            return query

        def _predict_score(self, instances):
            """Calls the AI Platform endpoint for the given model and instances and retrieves the scores."""
            start = time.time()
            endpoint = f"https://{MODEL_REGION}-ml.googleapis.com"
            client_options = ClientOptions(api_endpoint=endpoint)
            service = discovery.build(
                "ml", "v1", client_options=client_options, cache_discovery=False
            )
            name = f"projects/{GCP_PROJECT}/models/{self.model_name}"

            response = (
                service.projects()
                .predict(name=name, body={"instances": instances})
                .execute()
            )

            if "error" in response:
                raise RuntimeError(response["error"])

            log_duration("predict_score", start)
            return response["predictions"]

    class ColdStart:
        def __init__(self, Scoring):
            self.user = Scoring.user
            self.playlist_filters = Scoring.playlist_filters
            self.cold_start_categories = self.get_cold_start_categories()

        def get_scored_offers(self):
            order_query = (
                f"""ORDER BY (category in ({', '.join([f"'{category}'" for category in self.cold_start_categories])})) DESC, booking_number DESC"""
                if self.cold_start_categories
                else "ORDER BY booking_number DESC"
            )

            where_clause = (
                f"""(venue_id IN (SELECT "venue_id" FROM iris_venues_mv WHERE "iris_id" = :user_iris_id) OR is_national = True OR url IS NOT NULL)"""
                if self.user.iris_id
                else "is_national = True or url IS NOT NULL"
            )
            recommendations_query = text(
                f"""
                SELECT offer_id, category, url, product_id
                FROM {self.user.recommendable_offer_table}
                WHERE offer_id NOT IN
                    (
                        SELECT offer_id
                        FROM non_recommendable_offers
                        WHERE user_id = :user_id
                    )
                {self.playlist_filters}
                AND {where_clause}
                {order_query}
                LIMIT :number_of_preselected_offers;
                """
            )
            with create_db_connection() as connection:
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
                    "url": row[2],
                    "product_id": row[3],
                    "score": random.random(),
                }
                for row in query_result
            ]
            return cold_start_recommendations

        def get_cold_start_categories(self):
            qpi_answers_categories = list(MACRO_CATEGORIES_TYPE_MAPPING.keys())
            cold_start_query = text(
                f"""SELECT {'"' + '","'.join(qpi_answers_categories) + '"'} FROM qpi_answers WHERE user_id = :user_id;"""
            )

            with create_db_connection() as connection:
                query_result = connection.execute(
                    cold_start_query,
                    user_id=str(self.user.id),
                ).fetchall()

            cold_start_categories = []
            if len(query_result) == 0:
                return []
            for category_index, category in enumerate(query_result[0]):
                if category:
                    cold_start_categories.extend(
                        MACRO_CATEGORIES_TYPE_MAPPING[
                            qpi_answers_categories[category_index]
                        ]
                    )
            return list(set(cold_start_categories))
