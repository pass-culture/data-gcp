from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from loguru import logger
from model import DefaultClient, RecoClient, TextClient
from docarray import Document
import json

app = Flask(__name__)
CORS(app)


def load_model() -> DefaultClient:
    with open("./metadata/model_type.json", "r") as file:
        desc = json.load(file)
        if desc["type"] == "recommendation":
            return RecoClient(
                metric=desc["metric"],
                default_token=desc["default_token"],
                n_dim=desc["n_dim"],
            )
        if desc["type"] == "semantic":
            return TextClient(
                metric=desc["metric"],
                default_token=desc["default_token"],
                n_dim=desc["n_dim"],
                transformer=desc["transformer"],
            )
        else:
            raise Exception("Model desc not found.")


model = load_model()
model.load()
model.index()


def input_size(size):
    try:
        return int(size)
    except:
        return 10


def filter(selected_params, order_by: str, ascending: bool, size: int, debug: bool):
    try:
        results = model.filter(
            selected_params,
            details=debug,
            n=size,
            order_by=order_by,
            ascending=ascending,
        )
        return jsonify({"predictions": results})
    except Exception as e:
        logger.info(e)
        logger.info("error")
        return jsonify({"predictions": []})


def search_vector(vector: Document, size: int, selected_params, debug):
    try:
        if vector is not None:
            results = model.search(
                vector=vector, n=size, query_filter=selected_params, details=debug
            )
            return jsonify({"predictions": results})
        else:
            logger.info("item not found")
            return jsonify({"predictions": []})
    except Exception as e:
        logger.info(e)
        logger.info("error")
        return jsonify({"predictions": []})


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    input_json = request.get_json()["instances"][0]
    model_type = input_json["model_type"]
    debug = bool(input_json.get("debug", 0))
    selected_params = input_json.get("params", {})
    size = input_size(input_json.get("size", 500))

    try:
        if isinstance(model, RecoClient):
            if model_type == "recommendation":
                input_str = str(input_json["user_id"])
                vector = model.user_vector(input_str)
                return search_vector(vector, size, selected_params, debug)
        if isinstance(model, TextClient):
            if model_type == "semantic":
                input_str = str(input_json["text"])
                vector = model.text_vector(input_str)
                return search_vector(vector, size, selected_params, debug)

        if model_type == "similar_offer":
            input_str = str(input_json["offer_id"])
            vector = model.offer_vector(input_str)
            return search_vector(vector, size, selected_params, debug)
        if model_type == "filter":
            order_by = str(input_json["order_by"])
            ascending = bool(input_json["ascending"])
            return filter(selected_params, order_by, ascending, size, debug)

    except Exception as e:
        logger.info(e)
        logger.info("Wrong model")
        return jsonify({"predictions": []})


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
