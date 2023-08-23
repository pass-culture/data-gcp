import os
import re

from flask import Flask, jsonify, make_response, request
from flask_cors import CORS
from pcreco.utils.secrets.access_gcp_secrets import access_secret
from pcreco.utils.health_check_queries import get_materialized_view_status
from pcreco.utils.db.engine import create_connection, close_connection
from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.core.model_engine.recommendation import Recommendation
from pcreco.core.model_engine.similar_offer import SimilarOffer
from pcreco.models.reco.parser import (
    parse_params,
    parse_geolocation,
    parse_internal,
    parse_user,
)
import uuid

GCP_PROJECT = os.environ.get("GCP_PROJECT")
API_TOKEN_SECRET_ID = os.environ.get("API_TOKEN_SECRET_ID")
API_TOKEN = access_secret(GCP_PROJECT, API_TOKEN_SECRET_ID)

app = Flask(__name__)
CORS(
    app,
    resources={
        r"/*": {"origins": re.compile(os.environ.get("CORS_ALLOWED_ORIGIN", ".*"))}
    },
)


@app.before_request
def create_db_session():
    create_connection()


@app.teardown_request
def close_db_session(exception):
    close_connection()


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
    table_status = get_materialized_view_status("recommendable_offers_raw_mv")

    return jsonify(table_status), 200


@app.route("/health/non_recommendable_offers")
def health_check_non_recommendable_offers_status():
    table_status = get_materialized_view_status("non_recommendable_offers")

    return jsonify(table_status), 200


@app.route("/playlist_recommendation/<user_id>", methods=["GET", "POST"])
def playlist_recommendation(user_id: int):
    # unique id build for each call
    call_id = uuid.uuid4()
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403

    internal = parse_internal(request)
    longitude, latitude, geo_located = parse_geolocation(request)
    input_reco = parse_params(request, geo_located)
    user = User(user_id, call_id, longitude, latitude)
    scoring = Recommendation(user, params_in=input_reco)
    user_recommendations = scoring.get_scoring()

    if not internal:
        scoring.save_recommendation(user_recommendations)

    return jsonify(
        {
            "playlist_recommended_offers": user_recommendations,
            "params": {
                "reco_origin": scoring.reco_origin,
                "model_endpoint": scoring.model_params.name,
                "model_name": scoring.scorer.retrieval_endpoints[0].model_display_name,
                "model_version": scoring.scorer.retrieval_endpoints[0].model_version,
                "geo_located": geo_located,
                "filtered": input_reco.has_conditions if input_reco else False,
                "call_id": call_id,
            },
        }
    )


@app.route("/similar_offers/<offer_id>", methods=["GET", "POST"])
def similar_offers(offer_id: str):
    call_id = uuid.uuid4()
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403

    internal = parse_internal(request)
    longitude, latitude, geo_located = parse_geolocation(request)
    input_reco = parse_params(request, geo_located)
    user_id = parse_user(request)

    user = User(user_id, call_id, longitude, latitude)
    offer = Offer(offer_id, call_id, latitude, longitude)

    scoring = SimilarOffer(user, offer, params_in=input_reco)
    offer_recommendations = scoring.get_scoring()

    if len(offer_recommendations) == 0:
        # retrieve top offers when we don't have any offer to show
        input_reco.model_endpoint = "top_offers"
        scoring = Recommendation(user, params_in=input_reco)
        offer_recommendations = scoring.get_scoring()

    if not internal:
        scoring.save_recommendation(offer_recommendations)

    return jsonify(
        {
            "results": offer_recommendations,
            "params": {
                "reco_origin": scoring.reco_origin,
                "model_endpoint": scoring.model_params.name,
                "model_name": scoring.scorer.retrieval_endpoints[0].model_display_name,
                "model_version": scoring.scorer.retrieval_endpoints[0].model_version,
                "geo_located": geo_located,
                "filtered": input_reco.has_conditions if input_reco else False,
                "call_id": call_id,
            },
        }
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
