import os

from flask import Flask, jsonify, request

from recommendation import get_final_recommendations

APP_CONFIG = {
    "AB_TESTING_TABLE": os.environ.get("AB_TESTING_TABLE"),
    "API_TOKEN": os.environ.get("API_TOKEN"),
    "NUMBER_OF_RECOMMENDATIONS": 10,
    "MODEL_NAME": "poc_model",
    "MODEL_VERSION": "latest",
}

app = Flask(__name__)


@app.route("/check")
def check():
    return "OK"


@app.route("/recommendation/<user_id>")
def recommendation(user_id: int):
    token = request.args.get("token", None)
    longitude = request.args.get("longitude", None)
    latitude = request.args.get("latitude", None)

    if token != APP_CONFIG["API_TOKEN"]:
        return "Forbidden", 403

    recommendations = get_final_recommendations(
        user_id, longitude, latitude, APP_CONFIG
    )[: APP_CONFIG["NUMBER_OF_RECOMMENDATIONS"]]

    return jsonify({"recommended_offers": recommendations})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
