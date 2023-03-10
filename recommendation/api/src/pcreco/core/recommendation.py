# pylint: disable=invalid-name
import json
import random
from sqlalchemy import text
from pcreco.core.user import User
from pcreco.core.utils.cold_start import (
    get_cold_start_status,
    get_cold_start_categories,
)
from pcreco.core.utils.cold_start import (
    get_cold_start_status,
    get_cold_start_categories,
)
from pcreco.core.utils.mixing import (
    order_offers_by_score_and_diversify_features,
)
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.query_builder import RecommendableOffersQueryBuilder
from pcreco.utils.db.db_connection import get_session
from pcreco.core.utils.vertex_ai import predict_model
from pcreco.core.model_selection import select_reco_model_params
from pcreco.utils.env_vars import (
    NUMBER_OF_PRESELECTED_OFFERS,
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
        self.model_params = select_reco_model_params("cold_start_model")
        # TODO migrate this params in RecommendationDefaultModel model
        self.reco_radius = params_in.reco_radius
        self.is_reco_shuffled = params_in.is_reco_shuffled
        self.nb_reco_display = params_in.nb_reco_display
        self.is_reco_mixed = params_in.is_reco_mixed
        self.mixing_features = params_in.mixing_features
        self.include_digital = params_in.include_digital
        self.is_sort_by_distance = params_in.is_sort_by_distance
        self.reco_origin = "default"
        self.scoring = self.get_scoring_method()

    def get_scoring_method(self) -> object:
        # Force depending on model
        if self.model_params.force_cold_start:
            self.reco_origin = "cold_start"
            return self.ColdStart(self)
        if self.model_params.force_model:
            self.reco_origin = "algo"
            return self.Algo(self)
        if self.model_params.force_cold_start_model:
            self.reco_origin = "CS_algo"
            return self.Algo(self)
        # Normal behaviour
        if get_cold_start_status(self.user):
            self.reco_origin = "cold_start"
            return self.ColdStart(self)
        return self.Algo(self)

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

        return list(set([offer["id"] for offer in sorted_recommendations]))[
            : self.nb_reco_display
        ]

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
                        "group_id": self.model_params.name,
                        "reco_origin": self.reco_origin,
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
            self.model_params = scoring.model_params
            self.model_display_name = None
            self.model_version = None
            self.include_digital = scoring.include_digital
            self.recommendable_offers = self.get_recommendable_offers()
            self.cold_start_categories = get_cold_start_categories(self.user.id)

        def get_scored_offers(self) -> List[Dict[str, Any]]:
            start = time.time()
            if not len(self.recommendable_offers) > 0:
                log_duration(
                    f"no offers to score for {self.user.id} - {self.model_params.name}",
                    start,
                )
                return []
            else:
                if self.model_params.force_cold_start_model:
                    user_input = self.cold_start_categories
                else:
                    user_input = self.user.id
                log_duration(
                    f"user_input: {user_input}",
                    start,
                )

                instances = self._get_instances(user_input)

                predicted_scores = self._predict_score(instances)

                recommendations = [
                    {**recommendation, "score": predicted_scores[i][0]}
                    for i, recommendation in enumerate(self.recommendable_offers)
                ]

                log_duration(
                    f"scored {len(recommendations)} for {self.user.id} - {self.model_params.name}, ",
                    start,
                )
            return recommendations

        def _get_instances(self, user_input) -> List[Dict[str, str]]:
            user_to_rank = [user_input] * len(self.recommendable_offers)
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

            recommendations_query = RecommendableOffersQueryBuilder(
                self, RECOMMENDABLE_OFFER_LIMIT
            ).generate_query(order_query)

            query_result = []
            if recommendations_query is not None:
                connection = get_session()
                query_result = connection.execute(
                    text(recommendations_query),
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
                endpoint_name=self.model_params.endpoint_name,
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
            self.cold_start_categories = get_cold_start_categories(self.user.id)
            self.include_digital = scoring.include_digital
            self.model_version = None
            self.model_display_name = None

        def get_scored_offers(self) -> List[Dict[str, Any]]:
            order_query = (
                f"""(subcategory_id in ({', '.join([f"'{category}'" for category in self.cold_start_categories])})) DESC, booking_number DESC"""
                if len(self.cold_start_categories) > 0
                else "booking_number DESC"
            )

            recommendations_query = RecommendableOffersQueryBuilder(
                self, COLD_START_RECOMMENDABLE_OFFER_LIMIT
            ).generate_query(order_query)

            query_result = []
            if recommendations_query is not None:
                connection = get_session()
                query_result = connection.execute(
                    text(recommendations_query),
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
