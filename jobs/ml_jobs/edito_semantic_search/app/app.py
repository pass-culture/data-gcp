from flask import Flask
from flask_cors import CORS
from loguru import logger

from app.routes import api

app = Flask(__name__)
CORS(app)

app.register_blueprint(api)


if __name__ == "__main__":
    logger.info("startup", extra={"event": "startup", "response": "ready"})
    app.run()
