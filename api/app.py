import os

from flask import Flask, jsonify, request

from recommendation import (
    get_recommendations_for_user,
    order_offers_by_score_and_diversify_types,
    get_scored_recommendation_for_user,
)

API_TOKEN = os.environ.get("API_TOKEN")
NUMBER_OF_RECOMMENDATIONS = 10
MODEL_NAME = "pocmodel"
MODEL_VERSION = "v0"

app = Flask(__name__)


@app.route("/check")
def check():
    return "OK"


@app.route("/recommendation/<user_id>")
def recommendation(user_id: int):
    token = request.args.get("token", None)

    if token != API_TOKEN:
        return "Forbidden", 403

    recommendations_for_user = get_recommendations_for_user(
        user_id, NUMBER_OF_RECOMMENDATIONS
    )
    scored_recommendation_for_user = get_scored_recommendation_for_user(
        recommendations_for_user, MODEL_NAME, MODEL_VERSION
    )

    sorted_and_diversified_recommendations = order_offers_by_score_and_diversify_types(
        scored_recommendation_for_user
    )

    return jsonify({"recommended offers ": sorted_and_diversified_recommendations})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
