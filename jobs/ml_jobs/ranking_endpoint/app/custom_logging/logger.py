import json
import logging
import logging.config

with open("custom_logging/config.json", "r") as f:
    logging_config = json.load(f)
logging.config.dictConfig(logging_config)


logger = logging.getLogger("api")
