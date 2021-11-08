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

from cold_start import get_cold_start_status, get_cold_start_categories
from geolocalisation import get_iris_from_coordinates
from utils import (
    create_db_connection,
    log_duration,
    GCP_PROJECT,
    AB_TESTING_TABLE,
    NUMBER_OF_RECOMMENDATIONS,
    NUMBER_OF_PRESELECTED_OFFERS,
    MODEL_REGION,
    MODEL_NAME_A,
    MODEL_NAME_B,
    MODEL_NAME_C,
)


def get_final_recommendations(user_id: int, longitude: int, latitude: int) -> List[int]:

    request_response = query_ab_testing_table(user_id)
    if not request_response:
        group_id = ab_testing_assign_user(user_id)
    else:
        group_id = request_response[0]

    is_cold_start = get_cold_start_status(user_id)
    user_iris_id = get_iris_from_coordinates(longitude, latitude)

    if is_cold_start:
        reco_origin = "cold_start"
        cold_start_categories = get_cold_start_categories(user_id)
        scored_recommendation_for_user = get_cold_start_scored_recommendations_for_user(
            user_id,
            user_iris_id,
            cold_start_categories,
        )
    else:
        reco_origin = "algo"
        recommendations_for_user = get_intermediate_recommendations_for_user(
            user_id, user_iris_id
        )
        scored_recommendation_for_user = get_scored_recommendation_for_user(
            user_id, group_id, recommendations_for_user
        )

        # Keep the top 40 offers and shuffle them
        best_recommendations_for_user = sorted(
            scored_recommendation_for_user, key=lambda k: k["score"], reverse=True
        )[:40]
        for recommendation in best_recommendations_for_user:
            recommendation["score"] = random.random()

    final_recommendations = order_offers_by_score_and_diversify_categories(
        scored_recommendation_for_user
    )

    save_recommendation(user_id, final_recommendations, group_id, reco_origin)
    return final_recommendations


def query_ab_testing_table(
    user_id,
):
    start = time.time()

    with create_db_connection() as connection:
        request_response = connection.execute(
            text(f"SELECT groupid FROM {AB_TESTING_TABLE} WHERE userid= :user_id"),
            user_id=str(user_id),
        ).scalar()

    log_duration(f"query_ab_testing_table for {user_id}", start)
    return request_response


def ab_testing_assign_user(user_id):
    start = time.time()
    groups = ["A", "B", "C"]
    group_id = random.choice(groups)

    with create_db_connection() as connection:
        connection.execute(
            text(
                f"INSERT INTO {AB_TESTING_TABLE}(userid, groupid) VALUES (:user_id, :group_id)"
            ),
            user_id=user_id,
            group_id=str(group_id),
        )

    log_duration(f"ab_testing_assign_user for {user_id}", start)
    return group_id


def save_recommendation(
    user_id: int, recommendations: List[int], group_id: str, reco_origin: str
):
    start = time.time()
    date = datetime.datetime.now(pytz.utc)
    rows = []
    for offer_id in recommendations:
        rows.append(
            {
                "user_id": user_id,
                "offer_id": offer_id,
                "date": date,
                "group_id": group_id,
                "reco_origin": reco_origin,
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
    log_duration(f"save_recommendations for {user_id}", start)


def get_cold_start_scored_recommendations_for_user(
    user_id: int, user_iris_id: int, cold_start_categories: list
) -> List[Dict[str, Any]]:

    start = time.time()
    if cold_start_categories:
        order_query = f"""
            ORDER BY
                (category in ({', '.join([f"'{category}'" for category in cold_start_categories])})) DESC,
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
        SELECT offer_id, category, url, product_id
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
    log_duration(
        f"get_cold_start_scored_recommendations_for_user for {user_id} {'with localisation' if user_iris_id else ''}",
        start,
    )
    return cold_start_recommendations


def get_intermediate_recommendations_for_user(
    user_id: int, user_iris_id: int
) -> List[Dict[str, Any]]:

    start = time.time()
    if not user_iris_id:
        query = text(
            """
            SELECT offer_id, category, subcategory_id, url, item_id, product_id
            FROM recommendable_offers
            WHERE is_national = True or url IS NOT NULL
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )
            AND booking_number > 0
            ORDER BY RANDOM();
            """
        )

        with create_db_connection() as connection:
            query_result = connection.execute(query, user_id=str(user_id)).fetchall()

    else:
        query = text(
            f"""
            SELECT offer_id, category, subcategory_id, url, item_id, product_id
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
            AND booking_number > 0
            ORDER BY RANDOM();
            """
        )

        with create_db_connection() as connection:
            query_result = connection.execute(
                query, user_id=str(user_id), user_iris_id=str(user_iris_id)
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

    log_duration(
        f"get_intermediate_recommendations_for_user for {user_id} {'with localisation' if user_iris_id else ''}",
        start,
    )
    return user_recommendation


def get_scored_recommendation_for_user(
    user_id: int, group_id: str, user_recommendations: List[Dict[str, Any]]
) -> List[Dict[str, int]]:
    """
    Depending on the user group, prepare the data to send to the model, and make the call.
    """

    start = time.time()
    user_to_rank = [user_id] * len(user_recommendations)

    if group_id == "A":
        # 29/10/2021 : A = Algo v1
        model_name = MODEL_NAME_A
        offers_ids = [
            recommendation["item_id"] if recommendation["item_id"] else ""
            for recommendation in user_recommendations
        ]
        instances = [{"input_1": user_to_rank, "input_2": offers_ids}]
        # Format = dict with 2 inputs: arrays of users and offers

    elif group_id == "B":
        # 29/10/2021 : B = Algo v2 : Deep Reco
        model_name = MODEL_NAME_B
        offers_ids = [
            recommendation["item_id"] if recommendation["item_id"] else ""
            for recommendation in user_recommendations
        ]
        offers_subcategories = [
            recommendation["subcategory_id"] if recommendation["subcategory_id"] else ""
            for recommendation in user_recommendations
        ]

        instances = [
            {
                "input_1": user_to_rank,
                "input_2": offers_ids,
                "input_3": offers_subcategories,
            }
        ]
        # Format = dict with 3 inputs: arrays of users, offers and subcategories

    elif group_id == "C":
        # 29/10/2021 : C = Algo v2 : Matrix Factorization
        model_name = MODEL_NAME_C
        offers_ids = [
            recommendation["item_id"] if recommendation["item_id"] else ""
            for recommendation in user_recommendations
        ]
        instances = [{"input_1": user_to_rank, "input_2": offers_ids}]

    else:
        instances = []

    predicted_scores = predict_score(MODEL_REGION, GCP_PROJECT, model_name, instances)

    recommendations = [
        {**recommendation, "score": predicted_scores[i][0]}
        for i, recommendation in enumerate(user_recommendations)
    ]

    log_duration(
        f"get_scored_recommendation_for_user for {user_id} - {model_name}", start
    )
    return recommendations


def predict_score(region, project, model, instances):
    """
    Calls the AI Platform endpoint for the given model and instances and retrieves the scores.
    """
    start = time.time()
    endpoint = f"https://{region}-ml.googleapis.com"
    client_options = ClientOptions(api_endpoint=endpoint)
    service = discovery.build(
        "ml", "v1", client_options=client_options, cache_discovery=False
    )
    name = f"projects/{project}/models/{model}"

    response = (
        service.projects().predict(name=name, body={"instances": instances}).execute()
    )

    if "error" in response:
        raise RuntimeError(response["error"])

    log_duration(f"predict_score", start)
    return response["predictions"]


def order_offers_by_score_and_diversify_categories(
    offers: List[Dict[str, Any]]
) -> List[int]:
    """
    Group offers by category.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """

    start = time.time()
    offers_by_category = _get_offers_grouped_by_category(offers)

    offers_by_category_ordered_by_frequency = collections.OrderedDict(
        sorted(
            offers_by_category.items(),
            key=_get_number_of_offers_and_max_score_by_category,
            reverse=True,
        )
    )

    for offer_category in offers_by_category_ordered_by_frequency:
        offers_by_category_ordered_by_frequency[offer_category] = sorted(
            offers_by_category_ordered_by_frequency[offer_category],
            key=lambda k: k["score"],
            reverse=False,
        )

    diversified_offers = []
    while len(diversified_offers) != np.sum(
        [len(l) for l in offers_by_category.values()]
    ):
        for offer_category in offers_by_category_ordered_by_frequency.keys():
            if offers_by_category_ordered_by_frequency[offer_category]:
                diversified_offers.append(
                    offers_by_category_ordered_by_frequency[offer_category].pop()
                )
        if len(diversified_offers) >= NUMBER_OF_RECOMMENDATIONS:
            break

    ordered_and_diversified_offers = [offer["id"] for offer in diversified_offers][
        :NUMBER_OF_RECOMMENDATIONS
    ]

    log_duration("order_offers_by_score_and_diversify_categories", start)
    return ordered_and_diversified_offers


def _get_offers_grouped_by_category(offers: List[Dict[str, Any]]) -> Dict:
    start = time.time()
    offers_by_category = dict()
    product_ids = set()
    for offer in offers:
        offer_category = offer["category"]
        offer_product_id = offer["product_id"]
        if offer_category in offers_by_category.keys():
            if offer_product_id not in product_ids:
                offers_by_category[offer_category].append(offer)
                product_ids.add(offer_product_id)
        else:
            offers_by_category[offer_category] = [offer]

    log_duration("_get_offers_grouped_by_category", start)
    return offers_by_category


def _get_number_of_offers_and_max_score_by_category(
    category_and_offers: Tuple,
) -> Tuple:
    return (
        len(category_and_offers[1]),
        max([offer["score"] for offer in category_and_offers[1]]),
    )
