# pylint: disable=invalid-name
import json
import random
from sqlalchemy import text
from pcreco.core.user import User
from pcreco.core.utils.cold_start_status import get_cold_start_status
from pcreco.core.utils.mixing import (
    order_offers_by_score_and_diversify_features,
)
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import RecommendableOffersQueryBuilder
from pcreco.utils.db.db_connection import get_session
from pcreco.core.utils.vertex_ai import predict_model

from pcreco.utils.env_vars import (
    NUMBER_OF_PRESELECTED_OFFERS,
    ACTIVE_MODEL,
    RECO_ENDPOINT_NAME,
    AB_TESTING,
    AB_TEST_MODEL_DICT,
    RECOMMENDABLE_OFFER_LIMIT,
    COLD_START_RECOMMENDABLE_OFFER_LIMIT,
    log_duration,
)
import datetime
import time
import pytz
from typing import List, Dict, Any


class Recommendation:
    def __init__(self, user: User, params_in: PlaylistParamsIn):
        self.user = user
        self.json_input = params_in.json_input
        self.params_in_filters = params_in._get_conditions()
        self.params_in_model_name = params_in.model_name
        self.iscoldstart = (
            False if self.force_model else get_cold_start_status(self.user)
        )
        self.reco_radius = params_in.reco_radius
        self.is_reco_shuffled = params_in.is_reco_shuffled
        self.nb_reco_display = params_in.nb_reco_display
        self.is_reco_mixed = params_in.is_reco_mixed
        self.mixing_features = params_in.mixing_features
        self.include_numericals = params_in.include_numericals
        self.is_sort_by_distance = params_in.is_sort_by_distance
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
        # sort top offers per score and select 150 offers
        sorted_recommendations = sorted(
            self.scoring.get_scored_offers(),
            key=lambda k: k["score"],
            reverse=True,
        )[:NUMBER_OF_PRESELECTED_OFFERS]

        # apply diversification filter
        if self.is_reco_mixed:
            sorted_recommendations = order_offers_by_score_and_diversify_features(
                offers=sorted_recommendations,
                shuffle_recommendation=self.is_reco_shuffled,
                feature=self.mixing_features,
                nb_reco_display=self.nb_reco_display,
            )
        # order by distance if needed
        if self.is_sort_by_distance:
            sorted_recommendations = sorted(
                sorted_recommendations,
                key=lambda k: k["user_distance"],
                reverse=False,
            )

        return [offer["id"] for offer in sorted_recommendations][: self.nb_reco_display]

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

            connection = get_session()
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
            self.reco_radius = scoring.reco_radius
            self.model_name = scoring.model_name
            self.model_display_name = None
            self.model_version = None
            self.include_numericals = scoring.include_numericals
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

            user_recommendation = [
                {
                    "id": row[0],
                    "category": row[1],
                    "subcategory_id": row[2],
                    "search_group_name": row[3],
                    "item_id": row[4],
                    "user_distance": row[5],
                    "booking_number": row[6],
                }
                for row in query_result
            ]
            log_duration("get_recommendable_offers", start)
            return user_recommendation

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
            self.reco_radius = scoring.reco_radius
            self.cold_start_categories = self.get_cold_start_categories()
            self.include_numericals = scoring.include_numericals
            self.model_version = None
            self.model_display_name = None

        def get_scored_offers(self) -> List[Dict[str, Any]]:
            order_query = (
                f"""(subcategory_id in ({', '.join([f"'{category}'" for category in self.cold_start_categories])})) DESC, booking_number DESC"""
                if len(self.cold_start_categories) > 0
                else "booking_number DESC"
            )
            recommendations_query = text(
                RecommendableOffersQueryBuilder(
                    self, COLD_START_RECOMMENDABLE_OFFER_LIMIT
                ).generate_query(order_query)
            )

            connection = get_session()
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
                    "item_id": row[4],
                    "user_distance": row[5],
                    "booking_number": row[6],
                    "score": row[6],  # random.random()
                }
                for row in query_result
            ]

            return cold_start_recommendations

        def get_cold_start_categories(self) -> List[str]:
            cold_start_query = text(
                f"""SELECT subcategories FROM qpi_answers_mv WHERE user_id = :user_id;"""
            )

            connection = get_session()
            query_result = connection.execute(
                cold_start_query,
                user_id=str(self.user.id),
            ).fetchall()

            if len(query_result) == 0:
                return []
            cold_start_categories = [res[0] for res in query_result]
            return cold_start_categories
