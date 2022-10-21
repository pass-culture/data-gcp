# pylint: disable=invalid-name
import json
import random
from sqlalchemy import text
from pcreco.core.user import User
from pcreco.core.utils.cold_start_status import get_cold_start_status
from pcreco.core.utils.diversification import (
    order_offers_by_score_and_diversify_categories,
)
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.utils.db.db_connection import get_db
from pcreco.core.utils.vertex_ai import predict_model
from pcreco.utils.env_vars import (
    NUMBER_OF_PRESELECTED_OFFERS,
    ACTIVE_MODEL,
    RECO_ENDPOINT_NAME,
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


class Recommendation:
    def __init__(self, user: User, params_in: PlaylistParamsIn = None):
        self.user = user
        self.json_input = params_in.json_input if params_in else None
        self.params_in_filters = params_in._get_conditions() if params_in else ""
        self.params_in_model_name = params_in.model_name if params_in else None
        self.iscoldstart = (
            False if self.force_model else get_cold_start_status(self.user)
        )
        self.model_name = self.get_model_name()
        self.scoring = self.get_scoring_method()

    # rename force model
    @property
    def force_model(self) -> bool:
        if self.params_in_model_name:
            return True
        else:
            return False

    def get_model_name(self) -> str:
        if self.force_model:
            return self.params_in_model_name
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

    def get_scoring(self) -> List[str]:
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
                        "user_iris_id": self.user.iris_id,
                    }
                )

            connection = get_db()
            connection.execute(
                text(
                    """
                    INSERT INTO public.past_recommended_offers (userid, offerid, date, group_id, reco_origin, model_name, model_version, reco_filters, call_id, user_iris_id)
                    VALUES (:user_id, :offer_id, :date, :group_id, :reco_origin, :model_name, :model_version, :reco_filters, :call_id, :user_iris_id)
                    """
                ),
                rows,
            )
            log_duration(f"save_recommendations for {self.user.id}", start)

    class Algo:
        def __init__(self, scoring):
            self.user = scoring.user
            self.params_in_filters = scoring.params_in_filters
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
            start = time.time()
            query = text(self._get_intermediate_query())
            connection = get_db()
            query_result = connection.execute(
                query,
                user_id=str(self.user.id),
                user_iris_id=str(self.user.iris_id),
                user_longitude=float(self.user.longitude),
                user_latitude=float(self.user.latitude),
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
                }
                for row in query_result
            ]
            log_duration("get_recommendable_offers", start)
            return user_recommendation

        def _get_intermediate_query(self) -> str:
            geoloc_filter = (
                f"""( (ro.iris_id = :user_iris_id) OR is_national = True OR url IS NOT NULL)"""
                if self.user.iris_id
                else "(is_national = True or url IS NOT NULL)"
            )
            query = f"""
                WITH reco_offers_with_distance_to_user AS
                (
                SELECT  ro.offer_id,
                        ro.category,
                        ro.subcategory_id,
                        ro.search_group_name,
                        ro.url,
                        ro.url is not null as is_numerical,
                        ro.item_id,
                        ro.venue_id,
                        v.venue_latitude,
                        v.venue_longitude,
                        CASE
                                WHEN (              v.venue_latitude IS NOT NULL
                                            AND     "position" ='in' ) THEN st_distance(st_point(:user_longitude,:user_latitude)::geometry, st_point(v.venue_longitude,v.venue_latitude)::geometry, FALSE )
                                WHEN (              v.venue_latitude IS NOT NULL
                                            AND     "position" ='out' ) THEN ro.venue_distance_to_iris
                                ELSE NULL
                        END AS user_distance
                FROM  {self.user.recommendable_offer_table} ro
                left JOIN   (
                    SELECT  imv.venue_id,
                            imv.venue_latitude,
                            imv.venue_longitude,
                            imv.venue_point,
                            imv.iris_id
                    FROM   iris_venues_mv imv
                    WHERE  iris_id= :user_iris_id
                ) v ON ro.venue_id = v.venue_id 
                WHERE offer_id NOT IN (
                            SELECT offer_id
                            FROM   non_recommendable_offers
                            WHERE  user_id = :user_id
                        )
                AND {geoloc_filter}
                ),
                reco_offers_ranked_by_distance AS
                (
                    SELECT  ro.offer_id,
                            ro.category,
                            ro.subcategory_id,
                            ro.search_group_name,
                            ro.url,
                            ro.is_numerical,
                            ro.item_id,
                            row_number() OVER ( partition BY item_id ORDER BY user_distance ASC ) AS rank
                    FROM    reco_offers_with_distance_to_user ro 
                )
                SELECT * from reco_offers_ranked_by_distance where rank = 1 
                order by is_numerical ASC
                limit {RECOMMENDABLE_OFFER_LIMIT};
                """
            return query

        def _predict_score(self, instances) -> List[List[float]]:
            start = time.time()
            response = predict_model(
                endpoint_name=RECO_ENDPOINT_NAME,
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
            self.params_in_filters = scoring.params_in_filters
            self.cold_start_categories = self.get_cold_start_categories()
            self.model_version = None
            self.model_display_name = None

        def get_scored_offers(self) -> List[Dict[str, Any]]:
            order_query = (
                f"""ORDER BY (subcategory_id in ({', '.join([f"'{category}'" for category in self.cold_start_categories])})) DESC, booking_number DESC"""
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
                with reco_offers as(
                SELECT ro.offer_id, ro.category, ro.subcategory_id, ro.search_group_name, ro.url, ro.item_id, ro.venue_id, ro.booking_number
                FROM {self.user.recommendable_offer_table} ro
                WHERE offer_id NOT IN
                    (
                        SELECT offer_id
                        FROM non_recommendable_offers
                        WHERE user_id = :user_id
                    )
                {self.params_in_filters}
                AND {where_clause}
                ),
                reco_offers_with_distance_to_user as(
                    SELECT ro.offer_id, ro.category, ro.subcategory_id, ro.search_group_name, ro.url, ro.item_id, ro.venue_id, v.venue_latitude, v.venue_longitude,ro.booking_number,
                    CASE WHEN (v.venue_latitude is not null and :user_latitude is not null) THEN ST_Distance(ST_Point(:user_longitude,:user_latitude), ST_Point(v.venue_longitude, v.venue_latitude)) ELSE null END as user_distance
                    FROM reco_offers ro
                    LEFT JOIN (select ivm.venue_id,ivm.venue_latitude,ivm.venue_longitude from iris_venues_mv ivm where iris_id=:user_iris_id) v ON ro.venue_id = v.venue_id
                ),
                reco_offers_ranked_by_distance as(
                    SELECT ro.offer_id, ro.category, ro.subcategory_id, ro.search_group_name, ro.url, ro.item_id, ro.booking_number,
                    RANK() OVER (
                            PARTITION BY item_id
                            ORDER BY
                                user_distance ASC
                            ) as rank
                    FROM reco_offers_with_distance_to_user ro
                )
                SELECT  ro.offer_id, ro.category, ro.subcategory_id, ro.search_group_name, ro.url, ro.item_id
                FROM reco_offers_ranked_by_distance ro
                WHERE ro.rank=1
                {order_query}
                LIMIT :number_of_preselected_offers;
                """
            )
            connection = get_db()
            query_result = connection.execute(
                recommendations_query,
                user_iris_id=str(self.user.iris_id),
                user_id=str(self.user.id),
                user_longitude=float(self.user.longitude),
                user_latitude=float(self.user.latitude),
                number_of_preselected_offers=NUMBER_OF_PRESELECTED_OFFERS,
            ).fetchall()

            cold_start_recommendations = [
                {
                    "id": row[0],
                    "category": row[1],
                    "subcategory_id": row[2],
                    "search_group_name": row[3],
                    "url": row[4],
                    "item_id": row[5],
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
