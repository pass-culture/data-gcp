# pylint: disable=invalid-name
import json
import random
from sqlalchemy import text
from google.cloud import storage
from datetime import date
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
    NUMBER_OF_RECOMMENDATIONS,
    ACTIVE_MODEL,
    RECO_ENDPOINT_NAME,
    AB_TESTING,
    AB_TEST_MODEL_DICT,
    RECOMMENDABLE_OFFER_LIMIT,
    COLD_START_RECOMMENDABLE_OFFER_LIMIT,
    NUMBER_OF_PRESELECTED_OFFERS,
    NUMBER_OF_RECOMMENDATIONS,
    SHUFFLE_RECOMMENDATION,
    MIXING_RECOMMENDATION,
    MIXING_FEATURE,
    DEFAULT_RECO_RADIUS,
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

        self.reco_radius = params_in.reco_radius if params_in else DEFAULT_RECO_RADIUS
        self.reco_is_shuffle = (
            params_in.reco_is_shuffle if params_in else SHUFFLE_RECOMMENDATION
        )
        self.nb_reco_display = (
            params_in.nb_reco_display if params_in else NUMBER_OF_RECOMMENDATIONS
        )
        self.is_reco_mixed = (
            params_in.is_reco_mixed if params_in else MIXING_RECOMMENDATION
        )
        self.mixing_features = (
            params_in.mixing_features if params_in else MIXING_FEATURE
        )
        self.include_numericals = True

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
        if self.is_reco_mixed:
            final_recommendations = order_offers_by_score_and_diversify_features(
                offers=sorted(
                    self.scoring.get_scored_offers(),
                    key=lambda k: k["score"],
                    reverse=True,
                )[:NUMBER_OF_PRESELECTED_OFFERS],
                shuffle_recommendation=self.reco_is_shuffle,
                feature=self.mixing_features,
                nb_reco_display=self.nb_reco_display,
            )
        else:
            final_recommendations = sorted(
                self.scoring.get_scored_offers(), key=lambda k: k["score"], reverse=True
            )[: self.nb_reco_display]

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
                else "is_geolocated DESC, booking_number DESC"
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
                    "score": random.random(),
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

            if len(query_result) > 0:
                cold_start_categories = [res[0] for res in query_result]
            else:
                client = storage.Client()
                bucket = client.get_bucket("data-bucket-prod")
                todays_date = date.today().strftime("%Y%m%d")
                blob = bucket.get_blob(
                    f"QPI_exports/qpi_answers_{todays_date}/user_id_{self.user.id}.jsonl"
                )
                with blob.open("r") as f:
                    qpi_raw = json.load(f)
                if qpi_raw:
                    user_answer_ids = []
                    for answers in qpi_raw["answers"]:
                        for answers_id in answers["answer_ids"]:
                            user_answer_ids.append(answers_id)
                    cold_start_categories = user_answer_ids
                else:
                    cold_start_categories = []

            return cold_start_categories
