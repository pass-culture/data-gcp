import logging

import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

# from pythonjsonlogger import jsonlogger
# def get_logger() -> logging.Logger:
#     """Configure and return the logger."""
#     logger = logging.getLogger(__name__)
#     stdout = logging.StreamHandler(stream=sys.stdout)
#     formatter = jsonlogger.JsonFormatter(
#         "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
#         rename_fields={"levelname": "severity", "asctime": "timestamp"},
#     )
#     stdout.setFormatter(formatter)
#     logger.addHandler(stdout)
#     logger.setLevel(logging.INFO)
#     return logger
# logger = get_logger()


def get_logger() -> logging.Logger:
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client)
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


logger = get_logger()
logger.info("Info Log")
logger.warning("warning Log")
logger.debug("debug Log")
logger.error("error Log")
logger.critical("critical Log")
