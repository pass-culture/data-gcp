import os
import re

from flask import Flask, jsonify, make_response, request
from flask_cors import CORS
from pcreco.utils.secrets.access_gcp_secrets import access_secret
from pcreco.utils.health_check_queries import get_materialized_view_status
from pcreco.utils.db.engine import create_connection, close_connection
from pcreco.core.user import User
from pcreco.core.offer import Offer
from pcreco.core.recommendation import Recommendation
from pcreco.core.similar_offer import SimilarOffer
from pcreco.models.reco.parser import parse_params, parse_geolocation, parse_internal
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
    table_status = get_materialized_view_status(
        "recommendable_offers_per_iris_shape_mv"
    )

    return jsonify(table_status), 200


@app.route("/health/non_recommendable_offers")
def health_check_non_recommendable_offers_status():
    table_status = get_materialized_view_status("non_recommendable_offers")

    return jsonify(table_status), 200


@app.route("/health/iris_venues_mv")
def health_check_iris_venues_mv_status():
    table_status = get_materialized_view_status("iris_venues_mv")

    return jsonify(table_status), 200


@app.route("/playlist_recommendation/<user_id>", methods=["GET", "POST"])
def playlist_recommendation(user_id: int):
    # unique id build for each call
    call_id = uuid.uuid4()
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403

    internal = parse_internal(request)
    longitude, latitude, geo_located = parse_geolocation(request)
    input_reco = parse_params(request)

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
                "model_name": scoring.scoring.model_display_name,
                "model_version": scoring.scoring.model_version,
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
    input_reco = parse_params(request)
    user_id = request.args.get("user_id", -1)

    user = User(user_id, call_id, longitude, latitude)
    offer = Offer(offer_id, call_id, latitude, longitude)

    scoring = SimilarOffer(user, offer, params_in=input_reco)
    offer_recommendations = scoring.get_scoring()

    if not internal:
        scoring.save_recommendation(offer_recommendations)

    return jsonify(
        {
            "results": offer_recommendations,
            "params": {
                "reco_origin": scoring.reco_origin,
                "model_endpoint": scoring.model_params.name,
                "model_name": scoring.model_display_name,
                "model_version": scoring.model_version,
                "geo_located": geo_located,
                "filtered": input_reco.has_conditions if input_reco else False,
                "call_id": call_id,
            },
        }
    )


under_pat = re.compile(r"_([a-z])")


def underscore_to_camel(name):
    return under_pat.sub(lambda x: x.group(1).upper(), name)


@app.route("/test_pararms/<user_id>", methods=["GET", "POST"])
def test_params(user_id: int):
    if request.args.get("token", None) != API_TOKEN:
        return "Forbidden", 403
    params = None
    if request.method == "POST":
        params = dict(request.get_json(), **dict(request.args))
    elif request.method == "GET":
        params_args = request.args
        print("params_args: ", params_args)
        params_forms_to_dict = request.form.to_dict()
        print("params_forms_to_dict :", params_forms_to_dict)
    if params is None:
        params = {}
    else:
        params = {underscore_to_camel(k): v for k, v in params.items()}
    return jsonify(
        {"params": params},
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
