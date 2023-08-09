from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from pythonjsonlogger import jsonlogger
import logging
import sys
from model import DefaultClient, RecoClient, TextClient
from docarray import Document
import json


logger = logging.getLogger(__name__)
stdout = logging.StreamHandler(stream=sys.stdout)
fmt = jsonlogger.JsonFormatter(
    "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
    rename_fields={"levelname": "severity", "asctime": "timestamp"},
)

stdout.setFormatter(fmt)
logger.addHandler(stdout)
logger.setLevel(logging.INFO)

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
                n_dim=desc["n_dim"],
                transformer=desc["transformer"],
            )
        else:
            raise Exception("Model desc not found.")


log_data = {"event": "startup", "response": "load"}
logger.info("startup", extra=log_data)
model = load_model()
model.load()
model.index()
log_data = {"event": "startup", "response": "ready"}
logger.info("startup", extra=log_data)


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
        logger.exception(e)
        logger.info("error")
        return jsonify({"predictions": []})


def search_vector(vector: Document, size: int, selected_params, debug, item_id=None):
    try:
        if vector is not None:
            results = model.search(
                vector=vector,
                n=size,
                query_filter=selected_params,
                details=debug,
                item_id=item_id,
            )
            return jsonify({"predictions": results})
        else:
            logger.info("item not found")
            return jsonify({"predictions": []})
    except Exception as e:
        logger.exception(e)
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
            return search_vector(
                vector, size, selected_params, debug, item_id=input_str
            )

        if model_type == "filter":
            order_by = str(input_json["order_by"])
            ascending = bool(input_json["ascending"])
            return filter(selected_params, order_by, ascending, size, debug)

    except Exception as e:
        log_data = {"event": "error", "response": str(e)}
        logger.info("error", extra=log_data)
        logger.exception(e)

    return jsonify({"predictions": []})
