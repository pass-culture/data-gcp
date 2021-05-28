import collections
import datetime
import os
import random
from typing import Any, Dict, List, Tuple

import pytz
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from sqlalchemy import create_engine, engine, text

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
    user_id: int, longitude: int, latitude: int, app_config: Dict[str, Any]
) -> List[int]:

    ab_testing_table = app_config["AB_TESTING_TABLE"]
    connection = create_db_connection()

    request_response = connection.execute(
        text(f"SELECT groupid FROM {ab_testing_table} WHERE userid= :user_id"),
        user_id=str(user_id),
    ).scalar()

    if not request_response:
        group_id = "A" if random.random() > 0.5 else "B"
        connection.execute(
            text(
                f"INSERT INTO {ab_testing_table}(userid, groupid) VALUES (:user_id, :group_id)"
            ),
            user_id=user_id,
            group_id=str(group_id),
        )

    else:
        group_id = request_response[0]

    is_cold_start = get_cold_start_status(user_id, connection)
    user_iris_id = get_iris_from_coordinates(longitude, latitude, connection)

    if group_id == "A" and is_cold_start:
        cold_start_types = get_cold_start_types(user_id, connection)
        recommendations_for_user = get_cold_start_recommendations_for_user(
            user_id,
            user_iris_id,
            cold_start_types,
            app_config["NUMBER_OF_PRESELECTED_OFFERS"],
            connection,
        )

        final_recommendations = get_cold_start_final_recommendations(
            recommendations=recommendations_for_user,
            number_of_recommendations=app_config["NUMBER_OF_RECOMMENDATIONS"],
        )
    else:
        recommendations_for_user = get_intermediate_recommendations_for_user(
            user_id, user_iris_id, connection
        )
        scored_recommendation_for_user = get_scored_recommendation_for_user(
            recommendations_for_user,
            app_config["MODEL_REGION"],
            app_config["MODEL_NAME"],
            app_config["MODEL_VERSION"],
        )
        final_recommendations = order_offers_by_score_and_diversify_types(
            scored_recommendation_for_user
        )[: app_config["NUMBER_OF_RECOMMENDATIONS"]]

    save_recommendation(user_id, final_recommendations, connection)
    connection.close()
    return final_recommendations


def save_recommendation(user_id: int, recommendations: List[int], cursor):
    date = datetime.datetime.now(pytz.utc)

    for offer_id in recommendations:
        cursor.execute(
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


def get_cold_start_final_recommendations(
    recommendations: List[Dict[str, Any]],
    number_of_recommendations: int,
):
    cold_start_recommendations = [
        recommendation["id"] for recommendation in recommendations
    ]

    try:
        return random.sample(cold_start_recommendations, number_of_recommendations)
    except ValueError:
        return (
            []
        )  # not enough recommendable offers (happens often in dev because few bookings)


def get_cold_start_recommendations_for_user(
    user_id: int,
    user_iris_id: int,
    cold_start_types: list,
    number_of_preselected_offers: int,
    connection,
) -> List[Dict[str, Any]]:

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
        )
        """

    recommendations_query = text(
        f"""
        SELECT offer_id, type, url
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

    query_result = connection.execute(
        recommendations_query,
        user_iris_id=str(user_iris_id),
        user_id=str(user_id),
        number_of_preselected_offers=number_of_preselected_offers,
    ).fetchall()

    cold_start_recommendations = [
        {"id": row[0], "type": row[1], "url": row[2]} for row in query_result
    ]

    return cold_start_recommendations


def get_intermediate_recommendations_for_user(
    user_id: int,
    user_iris_id: int,
    connection,
) -> List[Dict[str, Any]]:

    if not user_iris_id:
        query = text(
            """
            SELECT offer_id, type, url, item_id
            FROM recommendable_offers
            WHERE is_national = True or url IS NOT NULL
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )
            ORDER BY RANDOM();
            """
        )
        query_result = connection.execute(query, user_id=str(user_id)).fetchall()
    else:
        query = text(
            """
            SELECT offer_id, type, url, item_id
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
                )
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )
            ORDER BY RANDOM();
            """
        )
        query_result = connection.execute(
            query, user_id=str(user_id), user_iris_id=str(user_iris_id)
        ).fetchall()

    user_recommendation = [
        {"id": row[0], "type": row[1], "url": row[2], "item_id": row[3]}
        for row in query_result
    ]

    return user_recommendation


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
        {**recommendation, "score": predicted_scores[i]}
        for i, recommendation in enumerate(user_recommendations)
    ]


def predict_score(region, project, model, instances, version):
    endpoint = f"https://{region}-ml.googleapis.com"
    client_options = ClientOptions(api_endpoint=endpoint)
    service = discovery.build(
        "ml", "v1", client_options=client_options, cache_discovery=False
    )
    name = f"projects/{project}/models/{model}"

    if version is not None:
        name += f"/versions/{version}"

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

    while len(diversified_offers) != len(offers):
        for offer_type in offers_by_type_ordered_by_frequency.keys():
            if offers_by_type_ordered_by_frequency[offer_type]:
                diversified_offers.append(
                    offers_by_type_ordered_by_frequency[offer_type].pop()
                )

    return [offer["id"] for offer in diversified_offers]


def _get_offers_grouped_by_type(offers: List[Dict[str, Any]]) -> Dict:
    offers_by_type = dict()
    for offer in offers:
        offer_type = offer["type"]
        if offer_type in offers_by_type.keys():
            offers_by_type[offer_type].append(offer)
        else:
            offers_by_type[offer_type] = [offer]
    return offers_by_type


def _get_number_of_offers_and_max_score_by_type(type_and_offers: Tuple) -> Tuple:
    return (
        len(type_and_offers[1]),
        max([offer["score"] for offer in type_and_offers[1]]),
    )
