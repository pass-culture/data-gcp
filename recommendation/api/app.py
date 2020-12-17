import os

from flask import Flask, jsonify, request

from health_check_queries import get_materialized_view_status
from recommendation import get_final_recommendations


API_TOKEN = os.environ.get("API_TOKEN")

APP_CONFIG = {
    "AB_TESTING_TABLE": "ab_testing_20201207",
    "NUMBER_OF_RECOMMENDATIONS": 10,
    "MODEL_NAME": "poc_model",
    "MODEL_VERSION": "latest",
}

app = Flask(__name__)


@app.route("/")
def home():
    return """
         __   __   __ __   __  o  __  
        |  ' (__) |  )  ) (__( | |  ) 
        Welcome to the recommendation API! 
        Check this route '/recommendation/<user_id>?token=<token>' for recommended offers. 
        ()_() 
        ( oo) 
    """


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
        user_id, longitude, latitude, APP_CONFIG
    )[: APP_CONFIG["NUMBER_OF_RECOMMENDATIONS"]]

    return jsonify({"recommended_offers": recommendations})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
