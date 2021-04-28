import collections
import datetime
import os
from random import random
from typing import Any, Dict, List, Tuple

import pytz
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from sqlalchemy import create_engine, engine

from access_gcp_secrets import access_secret
from cold_start import get_cold_start_status, get_cold_start_types
from geolocalisation import get_iris_from_coordinates

GCP_PROJECT = os.environ.get("GCP_PROJECT")

SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_SECRET_ID = os.environ.get("SQL_BASE_SECRET_ID")
SQL_BASE_SECRET_VERSION = os.environ.get("SQL_BASE_SECRET_VERSION")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")

SQL_BASE_PASSWORD = access_secret(
    GCP_PROJECT, SQL_BASE_SECRET_ID, SQL_BASE_SECRET_VERSION
)


query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)

engine = create_engine(
    engine.url.URL(
        drivername="postgres+pg8000",
        username=SQL_BASE_USER,
        password=SQL_BASE_PASSWORD,
        database=SQL_BASE,
        query=query_string,
    ),
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=1800,
)


def create_db_connection() -> Any:
    return engine.connect().execution_options(autocommit=True)


def get_final_recommendations(
    user_id: int,
    longitude: int,
    latitude: int,
    app_config: Dict[str, Any],
) -> List[int]:

    ab_testing_table = app_config["AB_TESTING_TABLE"]
    connection = create_db_connection()

    request_response = connection.execute(
        f"""SELECT groupid FROM {ab_testing_table} WHERE CAST(userid AS BIGINT)={user_id}"""
    ).scalar()

    if not request_response:
        group_id = "A" if random() > 0.5 else "B"
        connection.execute(
            f"""INSERT INTO {ab_testing_table}(userid, groupid) VALUES ({user_id}, '{group_id}')"""
        )

    else:
        group_id = request_response[0]

    if group_id == "A":
        is_cold_start = get_cold_start_status(user_id, connection)
        cold_start_types = (
            get_cold_start_types(user_id, connection) if is_cold_start else []
        )

        user_iris_id = get_iris_from_coordinates(longitude, latitude, connection)

        recommendations_for_user = get_intermediate_recommendations_for_user(
            user_id, user_iris_id, is_cold_start, cold_start_types, connection
        )

        if is_cold_start:
            scored_recommendation_for_user = [
                {**recommendation, "score": len(recommendations_for_user) - i}
                for i, recommendation in enumerate(recommendations_for_user)
            ]
            final_recommendations = get_cold_start_ordered_recommendations(
                recommendations=scored_recommendation_for_user,
                cold_start_types=cold_start_types,
                number_of_recommendations=app_config["NUMBER_OF_RECOMMENDATIONS"],
            )
        else:
            scored_recommendation_for_user = get_scored_recommendation_for_user(
                recommendations_for_user,
                app_config["MODEL_REGION"],
                app_config["MODEL_NAME"],
                app_config["MODEL_VERSION"],
            )
            final_recommendations = order_offers_by_score_and_diversify_types(
                scored_recommendation_for_user
            )[: app_config["NUMBER_OF_RECOMMENDATIONS"]]
    else:
        final_recommendations = []

    if final_recommendations:
        save_recommendation(user_id, final_recommendations, connection)

    connection.close()
    return final_recommendations


def save_recommendation(user_id: int, recommendations: List[int], cursor):
    date = datetime.datetime.now(pytz.utc)
    for offer_id in recommendations:
        row = (user_id, offer_id, date)
        cursor.execute(
            f"INSERT INTO public.past_recommended_offers "
            f"(userid, offerid, date) "
            f"VALUES (%s, %s, %s)",
            row,
        )


def get_cold_start_ordered_recommendations(
    recommendations: List[Dict[str, Any]],
    cold_start_types: List[str],
    number_of_recommendations: int,
):
    cold_start_types_recommendation = [
        recommendation
        for recommendation in recommendations
        if recommendation["type"] in cold_start_types
    ]
    other_recommendations = [
        recommendation
        for recommendation in recommendations
        if recommendation["type"] not in cold_start_types
    ]
    if len(cold_start_types_recommendation) >= number_of_recommendations:
        return order_offers_by_score_and_diversify_types(
            cold_start_types_recommendation
        )[:number_of_recommendations]

    missing_recommendations = number_of_recommendations - len(
        cold_start_types_recommendation
    )
    return (
        order_offers_by_score_and_diversify_types(cold_start_types_recommendation)
        + order_offers_by_score_and_diversify_types(other_recommendations)[
            :missing_recommendations
        ]
    )


def get_intermediate_recommendations_for_user(
    user_id: int,
    user_iris_id: int,
    is_cold_start: bool,
    cold_start_types: list,
    connection,
) -> List[Dict[str, Any]]:

    recommendations_query = get_recommendations_query(
        user_id, user_iris_id, is_cold_start, cold_start_types
    )
    query_result = connection.execute(recommendations_query).fetchall()

    user_recommendation = [
        {"id": row[0], "type": row[1], "url": row[2]} for row in query_result
    ]

    return user_recommendation


def get_recommendations_query(
    user_id: int, user_iris_id: int, is_cold_start: bool, cold_start_types: list
) -> str:
    if is_cold_start:
        if cold_start_types:
            order_query = f"""
                ORDER BY 
                    (type in ({', '.join([f"'{offer_type}'" for offer_type in cold_start_types])})) DESC, 
                    booking_number DESC
                 """
        else:
            order_query = "ORDER BY booking_number DESC"
    else:
        order_query = "ORDER BY RANDOM()"

    if not user_iris_id:
        query = f"""
            SELECT offer_id, type, url
            FROM recommendable_offers
            WHERE is_national = True or url IS NOT NULL
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE CAST(user_id AS BIGINT) = {user_id}
                )
            {order_query};
        """
    else:
        query = f"""
            SELECT offer_id, type, url
            FROM recommendable_offers
            WHERE
                (
                venue_id IN
                    (
                    SELECT "venue_id"
                    FROM iris_venues_mv
                    WHERE CAST("iris_id" AS BIGINT) = {user_iris_id}
                    )
                OR is_national = True
                )
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE CAST(user_id AS BIGINT) = {user_id}
                )
            {order_query};
        """
    return query


def get_scored_recommendation_for_user(
    user_recommendations: List[Dict[str, Any]],
    model_region: str,
    model_name: str,
    version: str,
) -> List[Dict[str, int]]:
    offers_ids = [recommendation["id"] for recommendation in user_recommendations]
    predicted_scores = predict_score(
        model_region, GCP_PROJECT, model_name, offers_ids, version
    )
    return [
        {
            **recommendation,
            "score": predicted_scores[i],
        }
        for i, recommendation in enumerate(user_recommendations)
    ]


def predict_score(region, project, model, instances, version):
    endpoint = f"https://{region}-ml.googleapis.com"
    client_options = ClientOptions(api_endpoint=endpoint)
    service = discovery.build(
        "ml", "v1", client_options=client_options, cache_discovery=False
    )
    name = "projects/{}/models/{}".format(project, model)

    if version is not None:
        name += "/versions/{}".format(version)

    response = (
        service.projects().predict(name=name, body={"instances": instances}).execute()
    )

    if "error" in response:
        raise RuntimeError(response["error"])

    return response["predictions"]


def order_offers_by_score_and_diversify_types(
    offers: List[Dict[str, Any]]
) -> List[int]:
    """
    Group offers by type.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """
    offers_by_type = _get_offers_grouped_by_type_and_onlineless(offers)

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

    while len(diversified_offers) != len(offers):
        for offer_type in offers_by_type_ordered_by_frequency.keys():
            if offers_by_type_ordered_by_frequency[offer_type]:
                diversified_offers.append(
                    offers_by_type_ordered_by_frequency[offer_type].pop()
                )

    return [offer["id"] for offer in diversified_offers]


def _get_offers_grouped_by_type_and_onlineless(offers: List[Dict[str, Any]]) -> Dict:
    offers_by_type = dict()
    for offer in offers:
        offer_type_and_onlineness = _get_offer_type_and_onlineness(offer)
        if offer_type_and_onlineness in offers_by_type.keys():
            offers_by_type[offer_type_and_onlineness].append(offer)
        else:
            offers_by_type[offer_type_and_onlineness] = [offer]
    return offers_by_type


def _get_number_of_offers_and_max_score_by_type(type_and_offers: Tuple) -> Tuple:
    return (
        len(type_and_offers[1]),
        max([offer["score"] for offer in type_and_offers[1]]),
    )


def _get_offer_type_and_onlineness(offer: Dict[str, Any]) -> str:
    return (
        str(offer["type"]) + "_DIGITAL"
        if offer["url"]
        else str(offer["type"]) + "_PHYSICAL"
    )
