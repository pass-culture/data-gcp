import os
import re

from flask import Flask, jsonify, make_response, request
from flask_cors import CORS
from sqlalchemy import create_engine, engine
from typing import Any

from access_gcp_secrets import access_secret
from health_check_queries import get_materialized_view_status
from recommendation import get_final_recommendations

GCP_PROJECT = os.environ.get("GCP_PROJECT")

API_TOKEN_SECRET_ID = os.environ.get("API_TOKEN_SECRET_ID")
API_TOKEN_SECRET_VERSION = os.environ.get("API_TOKEN_SECRET_VERSION")


API_TOKEN = access_secret(GCP_PROJECT, API_TOKEN_SECRET_ID, API_TOKEN_SECRET_VERSION)

APP_CONFIG = {
    "AB_TESTING_TABLE": os.environ.get("AB_TESTING_TABLE"),
    "NUMBER_OF_RECOMMENDATIONS": int(os.environ.get("NUMBER_OF_RECOMMENDATIONS", 10)),
    "NUMBER_OF_PRESELECTED_OFFERS": int(
        os.environ.get("NUMBER_OF_PRESELECTED_OFFERS", 50)
    ),
    "MODEL_REGION": os.environ.get("MODEL_REGION"),
    "MODEL_NAME_A": os.environ.get("MODEL_NAME_A"),
    "MODEL_VERSION_A": os.environ.get("MODEL_VERSION_A"),
    "MODEL_INPUT_A": os.environ.get("MODEL_INPUT_A"),
    "MODEL_NAME_B": os.environ.get("MODEL_NAME_B"),
    "MODEL_VERSION_B": os.environ.get("MODEL_VERSION_B"),
    "MODEL_INPUT_B": os.environ.get("MODEL_INPUT_B"),
}

app = Flask(__name__)
CORS(
    app,
    resources={
        r"/*": {"origins": re.compile(os.environ.get("CORS_ALLOWED_ORIGIN", ".*"))}
    },
)


def create_db_connection() -> Any:

    database = os.environ.get("SQL_BASE")
    username = os.environ.get("SQL_BASE_USER")
    sql_base_secret_id = os.environ.get("SQL_BASE_SECRET_ID")
    sql_base_secret_version = os.environ.get("SQL_BASE_SECRET_VERSION")
    sql_connection_name = os.environ.get("SQL_CONNECTION_NAME")
    if not username:
        return
    password = access_secret(GCP_PROJECT, sql_base_secret_id, sql_base_secret_version)

    query_string = dict(
        {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(sql_connection_name)}
    )

    connection_engine = create_engine(
        engine.url.URL(
            drivername="postgres+pg8000",
            username=username,
            password=password,
            database=database,
            query=query_string,
        ),
        pool_size=20,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )
    return connection_engine.connect().execution_options(autocommit=True)


connection = create_db_connection()


@app.route("/")
def home():
    response = make_response(
        """
         __   __   __ __   __  o  __
        |  ' (__) |  )  ) (__( | |  )

        Welcome to the recommendation API!
        Check this route '/recommendation/<user_id>?token=<token>' for recommended offers.

        ()_()
        ( oo)
    """
    )
    response.headers["content-type"] = "text/plain"
    return response


@app.route("/check")
def check():
    return "OK"


@app.route("/health/recommendable_offers")
def health_check_recommendable_offers_status():
    table_status = get_materialized_view_status("recommendable_offers")

    return jsonify(table_status), 200


@app.route("/health/non_recommendable_offers")
def health_check_non_recommendable_offers_status():
    table_status = get_materialized_view_status("non_recommendable_offers")

    return jsonify(table_status), 200


@app.route("/health/iris_venues_mv")
def health_check_iris_venues_mv_status():
    table_status = get_materialized_view_status("iris_venues_mv")

    return jsonify(table_status), 200


@app.route("/recommendation/<user_id>")
def recommendation(user_id: int):
    token = request.args.get("token", None)
    longitude = request.args.get("longitude", None)
    latitude = request.args.get("latitude", None)

    if token != API_TOKEN:
        return "Forbidden", 403

    recommendations = get_final_recommendations(
        user_id, longitude, latitude, APP_CONFIG, connection
    )

    return jsonify({"recommended_offers": recommendations})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
