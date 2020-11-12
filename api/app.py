import os

from flask import Flask, request


API_TOKEN = os.environ.get("API_TOKEN")

app = Flask(__name__)


@app.route("/")
def hello_world():
    return "Recommandation du jour : Star Wars!"


@app.route("/check")
def check():
    return "OK"


@app.route("/get_recommendations")
def get_recommendations():
    token = request.args.get("token", None)

    if token != API_TOKEN:
        return "Forbidden", 403

    return "Ok", 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
