import os
import re

from flask import Flask, jsonify, make_response, request
from flask_cors import CORS

from access_gcp_secrets import access_secret
from health_check_queries import get_materialized_view_status
from recommendation import get_final_recommendations

from refacto_api.src.user import User
from refacto_api.src.scoring import Scoring
from refacto_api.tools.playlist import Playlist

from utils import AB_TESTING, log_duration
import time

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

    recommendations, group_id, is_cold_start = get_final_recommendations(
        user_id, longitude, latitude
    )
    if is_cold_start:
        reco_origin = "cold_start"
    else:
        reco_origin = "algo"
    return jsonify(
        {
            "recommended_offers": recommendations,
            "AB_test": group_id,
            "reco_origin": reco_origin,
        }
    )


@app.route("/playlist_recommendation/<user_id>", methods=["GET", "POST"])
def playlist_recommendation(user_id: int):
    token = request.args.get("token", None)
    longitude = request.args.get("longitude", None)
    latitude = request.args.get("latitude", None)
    playlist_args_json = None
    if request.method == "POST":
        playlist_args_json = request.get_json()
        print(playlist_args_json)
    if token != API_TOKEN:
        return "Forbidden", 403

    recommendations, group_id, is_cold_start = get_final_recommendations(
        user_id, longitude, latitude, playlist_args_json
    )
    return jsonify(
        {
            "playlist_recommended_offers": recommendations,
        }
    )


@app.route("/refacto_api/<user_id>", methods=["GET", "POST"])
def refacto_api(user_id: int):
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403

    longitude = request.args.get("longitude", None)
    latitude = request.args.get("latitude", None)
    playlist_args_json = request.get_json() if request.method == "POST" else None
    user = User(user_id, longitude, latitude)
    playlist = Playlist(playlist_args_json) if playlist_args_json else None
    scoring = Scoring(user, Playlist=playlist)
    return jsonify(
        {
            "recommended_offers": scoring.get_recommendation(),
            "AB_test": user.group_id if AB_TESTING else None,
            "reco_origin": "cold_start" if scoring.iscoldstart else "algo",
        }
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
