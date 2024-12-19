import json
import logging

from google.cloud import logging as cloud_logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging.handlers.transports import BackgroundThreadTransport


class StructuredLogger(logging.LoggerAdapter):
    """A logger adapter to format log messages as structured JSON."""

    def process(self, msg, kwargs):
        # Extract severity from the log record
        log_level = kwargs.get("level", self.logger.level)
        severity_map = {
            logging.DEBUG: "DEBUG",
            logging.INFO: "INFO",
            logging.WARNING: "WARNING",
            logging.ERROR: "ERROR",
            logging.CRITICAL: "CRITICAL",
        }
        severity = severity_map.get(log_level, "DEFAULT")

        # Return a structured JSON message
        return json.dumps({"message": msg, "severity": severity}), kwargs


def setup_logger():
    # Create the Google Cloud Logging client
    client = cloud_logging.Client()

    # Create a Cloud Logging handler
    cloud_handler = CloudLoggingHandler(client, transport=BackgroundThreadTransport)

    # Set up a standard Python logger
    base_logger = logging.getLogger("hypercorn")
    base_logger.setLevel(logging.INFO)  # Set base level to INFO
    base_logger.addHandler(cloud_handler)

    # Add a console handler for local debugging (optional)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    base_logger.addHandler(console_handler)

    # Wrap the logger with the StructuredLogger
    structured_logger = StructuredLogger(base_logger, {})
    return structured_logger


# Setup the structured logger
logger = setup_logger()

# Test logging with different levels
logger.debug("TEST: This is a debug message.")
logger.info("TEST: This is an info message.")
logger.warning("TEST: This is a warning message.")
logger.error("TEST: This is an error message.")
logger.critical("TEST: This is a critical message.")
