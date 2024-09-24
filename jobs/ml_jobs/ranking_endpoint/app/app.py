import logging
import sys

from flask import Flask, Response, jsonify, request
from flask_cors import CORS
from model import PredictPipeline
from pythonjsonlogger import jsonlogger

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

model = PredictPipeline()


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    input_json = None
    results = None
    try:
        input_json = request.get_json()["instances"]
        results = model.predict(input_json)

        return jsonify({"predictions": results}), 200

    except Exception as e:
        log_data = {"event": "predict", "result": results, "request": request.json()}

        log_data = {"event": "error", "exception": str(e)}
        logger.error(log_data)
        logger.exception(e)

        return jsonify({"predictions": []}), 500


if __name__ == "__main__":
    app.run()
