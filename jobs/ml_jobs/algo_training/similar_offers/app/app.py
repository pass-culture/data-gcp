from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from loguru import logger
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from model import RiiModel
import gc
from logging import config
import logging
from flask_google_cloud_logger import FlaskGoogleCloudLogger

LOG_CONFIG = {
    "version": 1,
    "formatters": {
        "json": {
            "()": "flask_google_cloud_logger.FlaskGoogleCloudFormatter",
            "application_info": {
                "type": "python-application",
                "application_name": "[Data](similar_offers) API",
            },
            "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
        }
    },
    "handlers": {"json": {"class": "logging.StreamHandler", "formatter": "json"}},
    "loggers": {
        "root": {"level": "INFO", "handlers": ["json"]},
        "werkzeug": {
            "level": "WARN",  # Disable werkzeug hardcoded logger
            "handlers": ["json"],
        },
    },
}

config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("root")

app = Flask(__name__)
CORS(app)
FlaskGoogleCloudLogger(app)

rii_model = RiiModel()
rii_model.set_up_model()
rii_model.set_up_item_indexes()
rii_model.load("./metadata/rii.pkl")


def flush_memory():
    gc.collect()


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    req_json = request.get_json()
    input_json = req_json["instances"]
    item_id = input_json[0]["offer_id"]
    selected_categories = input_json[0].get("selected_categories", [])
    selected_offers = input_json[0].get("selected_offers", [])

    if selected_categories is not None and len(selected_categories) > 0:
        selected_idx = rii_model.get_idx_from_categories(selected_categories)
    elif selected_offers is not None and len(selected_offers) > 0:
        selected_idx = rii_model.selected_to_idx(selected_offers)
    else:
        selected_idx = None

    try:
        n = int(input_json[0].get("size", 10))
    except:
        n = 10
    try:
        sim_offers = rii_model.compute_distance(item_id, selected_idx=selected_idx, n=n)
        return jsonify(sim_offers)
    except Exception as e:
        logger.info(e)
        logger.info("error")
        return jsonify({"predictions": []})


scheduler = BackgroundScheduler()
scheduler.add_job(func=flush_memory, trigger="interval", seconds=300)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
