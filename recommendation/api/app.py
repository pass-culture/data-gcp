import os
import re

from flask import Flask, jsonify, make_response, request
from flask_cors import CORS

from src.pcreco.utils.secrets.access_gcp_secrets import access_secret
from src.pcreco.utils.health_check_queries import get_materialized_view_status

from src.pcreco.core.user import User
from src.pcreco.core.scoring import Scoring
from src.pcreco.models.reco.RecommendationIn import RecommendationIn

from src.pcreco.utils.env_vars import AB_TESTING, log_duration
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
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403

    longitude = request.args.get("longitude", None)
    latitude = request.args.get("latitude", None)
    post_args_json = request.get_json() if request.method == "POST" else None

    user = User(user_id, longitude, latitude)
    input_reco = RecommendationIn(post_args_json) if post_args_json else None
    scoring = Scoring(user, RecommendationIn=input_reco)

    user_recommendations = scoring.get_recommendation()
    scoring.save_recommendation(user_recommendations)
    return jsonify(
        {
            "recommended_offers": user_recommendations,
            "AB_test": user.group_id if AB_TESTING else None,
            "reco_origin": "cold_start" if scoring.iscoldstart else "algo",
            "model_name": scoring.model_name if not scoring.iscoldstart else None,
        }
    )


@app.route("/playlist_recommendation/<user_id>", methods=["GET", "POST"])
def playlist_recommendation(user_id: int):
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403

    longitude = request.args.get("longitude", None)
    latitude = request.args.get("latitude", None)
    post_args_json = request.get_json() if request.method == "POST" else None

    user = User(user_id, longitude, latitude)
    input_reco = RecommendationIn(post_args_json) if post_args_json else None
    scoring = Scoring(user, RecommendationIn=input_reco)

    user_recommendations = scoring.get_recommendation()
    scoring.save_recommendation(user_recommendations)
    return jsonify(
        {
            "playlist_recommended_offers": user_recommendations,
        }
    )


@app.route("/pcreco/<user_id>", methods=["GET", "POST"])
def pcreco(user_id: int):
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403

    longitude = request.args.get("longitude", None)
    latitude = request.args.get("latitude", None)
    post_args_json = request.get_json() if request.method == "POST" else None

    user = User(user_id, longitude, latitude)
    input_reco = RecommendationIn(post_args_json) if post_args_json else None
    scoring = Scoring(user, RecommendationIn=input_reco)

    user_recommendations = scoring.get_recommendation()
    scoring.save_recommendation(user_recommendations)
    return jsonify(
        {
            "recommended_offers": user_recommendations,
            "AB_test": user.group_id if AB_TESTING else None,
            "reco_origin": "cold_start" if scoring.iscoldstart else "algo",
            "model_name": scoring.model_name if not scoring.iscoldstart else None,
        }
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
