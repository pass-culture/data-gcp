import os
import re

from flask import Flask, jsonify, make_response, request
from flask_cors import CORS

from access_gcp_secrets import access_secret
from health_check_queries import get_materialized_view_status
from recommendation import get_final_recommendations

GCP_PROJECT = os.environ.get("GCP_PROJECT")

API_TOKEN_SECRET_ID = os.environ.get("API_TOKEN_SECRET_ID")
API_TOKEN_SECRET_VERSION = os.environ.get("API_TOKEN_SECRET_VERSION")


API_TOKEN = access_secret(GCP_PROJECT, API_TOKEN_SECRET_ID, API_TOKEN_SECRET_VERSION)

app = Flask(__name__)
CORS(
    app,
    resources={
        r"/*": {"origins": re.compile(os.environ.get("CORS_ALLOWED_ORIGIN", ".*"))}
    },
)


@app.route("/")
def home():
    response = make_response(
        """
        PassCulture - Recommendation API

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

    recommendations = get_final_recommendations(user_id, longitude, latitude)

    return jsonify({"recommended_offers": recommendations})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
