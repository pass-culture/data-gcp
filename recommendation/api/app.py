import os

from flask import Flask, jsonify, request, make_response

from health_check_queries import get_materialized_view_status
from recommendation import get_final_recommendations

from google.cloud import secretmanager


def access_secret_version(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

API_TOKEN_SECRET_ID = os.environ.get("API_TOKEN_SECRET_ID")
API_TOKEN_SECRET_VERSION = os.environ.get("API_TOKEN_SECRET_VERSION")

API_TOKEN = access_secret_version(
    GCP_PROJECT_ID, API_TOKEN_SECRET_ID, API_TOKEN_SECRET_VERSION
)


APP_CONFIG = {
    "AB_TESTING_TABLE": os.environ.get("AB_TESTING_TABLE"),
    "NUMBER_OF_RECOMMENDATIONS": os.environ.get("NUMBER_OF_RECOMMENDATIONS"),
    "MODEL_NAME": os.environ.get("MODEL_NAME"),
    "MODEL_VERSION": os.environ.get("MODEL_VERSION"),
}

app = Flask(__name__)


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
        user_id,
        longitude,
        latitude,
        APP_CONFIG,
    )

    return jsonify({"recommended_offers": recommendations})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
