import collections
import datetime
import random
import time
from typing import Any, Dict, List, Tuple

from sqlalchemy import text
import numpy as np
import pytz

from google.api_core.client_options import ClientOptions
from googleapiclient import discovery

from cold_start import get_cold_start_status, get_cold_start_types
from geolocalisation import get_iris_from_coordinates
from utils import create_db_connection, log_duration, GCP_PROJECT


def get_final_recommendations(
    user_id: int, longitude: int, latitude: int, app_config: Dict[str, Any]
) -> List[int]:
    request_response = query_ab_testing_table(user_id, app_config)
    if not request_response:
        group_id = ab_testing_assign_user(user_id, app_config)
    else:
        group_id = request_response[0]

    is_cold_start = get_cold_start_status(user_id)
    user_iris_id = get_iris_from_coordinates(longitude, latitude)

    if group_id == "A" and is_cold_start:
        cold_start_types = get_cold_start_types(user_id)
        scored_recommendation_for_user = get_cold_start_scored_recommendations_for_user(
            user_id,
            user_iris_id,
            cold_start_types,
            app_config["NUMBER_OF_PRESELECTED_OFFERS"],
        )
    else:
        filter_unbooked_offers = group_id == "A"
        recommendations_for_user = get_intermediate_recommendations_for_user(
            user_id, user_iris_id, filter_unbooked_offers
        )
        scored_recommendation_for_user = get_scored_recommendation_for_user(
            user_id,
            recommendations_for_user,
            app_config["MODEL_REGION"],
            app_config[f"MODEL_NAME_{group_id}"],
            app_config[f"MODEL_VERSION_{group_id}"],
            app_config[f"MODEL_INPUT_{group_id}"],
        )

        if group_id == "A":
            scored_recommendation_for_user = sorted(
                scored_recommendation_for_user, key=lambda k: k["score"], reverse=True
            )[:40]
            for recommendation in scored_recommendation_for_user:
                recommendation["score"] = random.random()

    final_recommendations = order_offers_by_score_and_diversify_types(
        scored_recommendation_for_user, app_config["NUMBER_OF_RECOMMENDATIONS"]
    )

    save_recommendation(user_id, final_recommendations)
    return final_recommendations


def query_ab_testing_table(user_id, app_config):
    start = time.time()
    ab_testing_table = app_config["AB_TESTING_TABLE"]

    with create_db_connection() as connection:
        request_response = connection.execute(
            text(f"SELECT groupid FROM {ab_testing_table} WHERE userid= :user_id"),
            user_id=str(user_id),
        ).scalar()

    log_duration(f"query_ab_testing_table for {user_id}", start)
    return request_response


def ab_testing_assign_user(user_id, app_config):
    start = time.time()
    ab_testing_table = app_config["AB_TESTING_TABLE"]
    group_id = "A" if random.random() > 0.5 else "B"

    with create_db_connection() as connection:
        connection.execute(
            text(
                f"INSERT INTO {ab_testing_table}(userid, groupid) VALUES (:user_id, :group_id)"
            ),
            user_id=user_id,
            group_id=str(group_id),
        )

    log_duration(f"ab_testing_assign_user for {user_id}", start)
    return group_id


def save_recommendation(user_id: int, recommendations: List[int]):
    start = time.time()
    date = datetime.datetime.now(pytz.utc)

    with create_db_connection() as connection:
        for offer_id in recommendations:
            connection.execute(
                text(
                    """
                    INSERT INTO public.past_recommended_offers(userid, offerid, date)
                    VALUES (:user_id, :offer_id, :date)
                    """
                ),
                user_id=user_id,
                offer_id=offer_id,
                date=date,
            )
    log_duration(f"save_recommendations for {user_id}", start)


def get_cold_start_scored_recommendations_for_user(
    user_id: int,
    user_iris_id: int,
    cold_start_types: list,
    number_of_preselected_offers: int,
) -> List[Dict[str, Any]]:

    start = time.time()
    if cold_start_types:
        order_query = f"""
            ORDER BY
                (type in ({', '.join([f"'{offer_type}'" for offer_type in cold_start_types])})) DESC,
                booking_number DESC
            """
    else:
        order_query = "ORDER BY booking_number DESC"

    if not user_iris_id:
        where_clause = "is_national = True or url IS NOT NULL"
    else:
        where_clause = """
        (
            venue_id IN
                (
                    SELECT "venue_id"
                    FROM iris_venues_mv
                    WHERE "iris_id" = :user_iris_id
                )
            OR is_national = True
            OR url IS NOT NULL
        )
        """

    recommendations_query = text(
        f"""
        SELECT offer_id, type, url, product_id
        FROM recommendable_offers
        WHERE offer_id NOT IN
            (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
            )
        AND {where_clause}
        AND booking_number > 0
        {order_query}
        LIMIT :number_of_preselected_offers;
        """
    )

    with create_db_connection() as connection:
        query_result = connection.execute(
            recommendations_query,
            user_iris_id=str(user_iris_id),
            user_id=str(user_id),
            number_of_preselected_offers=number_of_preselected_offers,
        ).fetchall()

    cold_start_recommendations = [
        {
            "id": row[0],
            "type": row[1],
            "url": row[2],
            "product_id": row[3],
            "score": random.random(),
        }
        for row in query_result
    ]
    log_duration(
        f"get_cold_start_scored_recommendations_for_user for {user_id} {'with localisation' if user_iris_id else ''}",
        start,
    )
    return cold_start_recommendations


def get_intermediate_recommendations_for_user(
    user_id: int, user_iris_id: int, filter_unbooked_offers: bool
) -> List[Dict[str, Any]]:

    start = time.time()
    unbooked_offers_filter = "AND booking_number > 0 " if filter_unbooked_offers else ""
    if not user_iris_id:
        query = text(
            f"""
            SELECT offer_id, type, url, item_id, product_id
            FROM recommendable_offers
            WHERE is_national = True or url IS NOT NULL
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )
            {unbooked_offers_filter}ORDER BY RANDOM();
            """
        )

        with create_db_connection() as connection:
            query_result = connection.execute(query, user_id=str(user_id)).fetchall()

    else:
        query = text(
            f"""
            SELECT offer_id, type, url, item_id, product_id
            FROM recommendable_offers
            WHERE
                (
                venue_id IN
                    (
                    SELECT "venue_id"
                    FROM iris_venues_mv
                    WHERE "iris_id" = :user_iris_id
                    )
                OR is_national = True
                OR url IS NOT NULL
                )
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )
            {unbooked_offers_filter}ORDER BY RANDOM();
            """
        )

        with create_db_connection() as connection:
            query_result = connection.execute(
                query, user_id=str(user_id), user_iris_id=str(user_iris_id)
            ).fetchall()

    user_recommendation = [
        {
            "id": row[0],
            "type": row[1],
            "url": row[2],
            "item_id": row[3],
            "product_id": row[4],
        }
        for row in query_result
    ]

    log_duration(
        f"get_intermediate_recommendations_for_user for {user_id} {'with localisation' if user_iris_id else ''}",
        start,
    )
    return user_recommendation


def get_scored_recommendation_for_user(
    user_id: int,
    user_recommendations: List[Dict[str, Any]],
    model_region: str,
    model_name: str,
    version: str,
    input_type: str,
) -> List[Dict[str, int]]:

    start = time.time()
    user_to_rank = [user_id for reco in user_recommendations]
    if input_type == "offer_id_list":
        instances = [recommendation["id"] for recommendation in user_recommendations]
    elif input_type == "item_id_and_user_id_lists":
        offers_ids = [
            recommendation["item_id"] if recommendation["item_id"] else ""
            for recommendation in user_recommendations
        ]
        instances = [{"input_1": user_to_rank, "input_2": offers_ids}]
    else:
        instances = []
    predicted_scores = predict_score(
        model_region, GCP_PROJECT, model_name, instances, version
    )

    recommendations = [
        {**recommendation, "score": predicted_scores[i]}
        for i, recommendation in enumerate(user_recommendations)
    ]

    log_duration(
        f"get_scored_recommendation_for_user for {user_id} - {model_name}", start
    )
    return recommendations


def predict_score(region, project, model, instances, version):
    start = time.time()
    endpoint = f"https://{region}-ml.googleapis.com"
    client_options = ClientOptions(api_endpoint=endpoint)
    service = discovery.build(
        "ml", "v1", client_options=client_options, cache_discovery=False
    )
    name = f"projects/{project}/models/{model}"
    if version:
        name += f"/versions/{version}"

    response = (
        service.projects().predict(name=name, body={"instances": instances}).execute()
    )

    if "error" in response:
        raise RuntimeError(response["error"])

    log_duration(f"predict_score", start)
    return response["predictions"]


def order_offers_by_score_and_diversify_types(
    offers: List[Dict[str, Any]], number_of_recommendations: int
) -> List[int]:
    """
    Group offers by type.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """

    start = time.time()
    offers_by_type = _get_offers_grouped_by_type(offers)

    offers_by_type_ordered_by_frequency = collections.OrderedDict(
        sorted(
            offers_by_type.items(),
            key=_get_number_of_offers_and_max_score_by_type,
            reverse=True,
        )
    )

    for offer_type in offers_by_type_ordered_by_frequency:
        offers_by_type_ordered_by_frequency[offer_type] = sorted(
            offers_by_type_ordered_by_frequency[offer_type],
            key=lambda k: k["score"],
            reverse=False,
        )

    diversified_offers = []
    while len(diversified_offers) != np.sum([len(l) for l in offers_by_type.values()]):
        for offer_type in offers_by_type_ordered_by_frequency.keys():
            if offers_by_type_ordered_by_frequency[offer_type]:
                diversified_offers.append(
                    offers_by_type_ordered_by_frequency[offer_type].pop()
                )
        if len(diversified_offers) >= number_of_recommendations:
            break

    ordered_and_diversified_offers = [offer["id"] for offer in diversified_offers][
        :number_of_recommendations
    ]

    log_duration(f"order_offers_by_score_and_diversify_types", start)
    return ordered_and_diversified_offers


def _get_offers_grouped_by_type(offers: List[Dict[str, Any]]) -> Dict:
    start = time.time()
    offers_by_type = dict()
    product_ids = set()
    for offer in offers:
        offer_type = offer["type"]
        offer_product_id = offer["product_id"]
        if offer_type in offers_by_type.keys():
            if offer_product_id not in product_ids:
                offers_by_type[offer_type].append(offer)
                product_ids.add(offer_product_id)
        else:
            offers_by_type[offer_type] = [offer]

    log_duration("_get_offers_grouped_by_type", start)
    return offers_by_type


def _get_number_of_offers_and_max_score_by_type(type_and_offers: Tuple) -> Tuple:
    return (
        len(type_and_offers[1]),
        max([offer["score"] for offer in type_and_offers[1]]),
    )
