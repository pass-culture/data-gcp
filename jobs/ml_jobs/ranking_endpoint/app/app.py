from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from model import PredictPipeline
from pythonjsonlogger import jsonlogger
import logging
import sys

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
    input_json = request.get_json()["instances"]
    try:
        results = model.predict(input_json)

        log_data = {
            "event": "predict",
            "input_data": input_json,
            "result": results,
        }
        logger.info("predict.", extra=log_data)

        return jsonify(
            {"predictions": sorted(results, key=lambda x: x["score"], reverse=True)}
        )

    except Exception as e:
        log_data = {"event": "error", "exception": str(e)}
        logger.info(log_data)
        logger.exception(e)

    return jsonify({"predictions": []})


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
