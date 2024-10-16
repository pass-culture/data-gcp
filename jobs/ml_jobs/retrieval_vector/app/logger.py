import logging
import sys

from pythonjsonlogger import jsonlogger


def get_logger() -> logging.Logger:
    """Configure and return the logger."""
    logger = logging.getLogger(__name__)
    stdout = logging.StreamHandler(stream=sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
        rename_fields={"levelname": "severity", "asctime": "timestamp"},
    )
    stdout.setFormatter(formatter)
    logger.addHandler(stdout)
    logger.setLevel(logging.INFO)

    return logger


logger = get_logger()
