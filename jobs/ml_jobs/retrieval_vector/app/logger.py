import logging
import sys

from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def process_log_record(self, log_record):
        # Ensure 'severity' is at the top level
        if "severity" not in log_record and "levelname" in log_record:
            log_record["severity"] = log_record["levelname"]
        return super().process_log_record(log_record)


def get_logger() -> logging.Logger:
    """Configure and return the logger."""
    logger = logging.getLogger(__name__)
    stdout = logging.StreamHandler(stream=sys.stdout)
    formatter = CustomJsonFormatter(
        "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
        rename_fields={"levelname": "severity", "asctime": "timestamp"},
    )
    stdout.setFormatter(formatter)
    logger.addHandler(stdout)
    logger.setLevel(logging.INFO)
    return logger


logger = get_logger()

logger.info("Info Log")
logger.warning("Warning Log")
logger.debug("Debug Log")
logger.error("Error Log")
logger.critical("Critical Log")
