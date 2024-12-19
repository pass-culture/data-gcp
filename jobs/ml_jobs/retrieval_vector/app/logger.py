import json
import logging
import sys

from google.cloud import logging as cloud_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler


def setup_logger():
    class StructuredLogHandler(logging.StreamHandler):
        def emit(self, record):
            log_entry = {"message": record.getMessage(), "severity": record.levelname}
            self.stream.write(json.dumps(log_entry) + "\n")

    # Initialize the Cloud Logging client
    client = cloud_logging.Client()
    cloud_handler = CloudLoggingHandler(client)

    # Create a logger
    logger = logging.getLogger("cloudLogger")
    logger.setLevel(logging.DEBUG)  # Capture all levels of logging

    # Create a structured log handler for stdout
    structured_handler = StructuredLogHandler(stream=sys.stdout)
    structured_handler.setLevel(logging.DEBUG)  # Capture all levels of logging

    # Add the handlers to the logger
    logger.addHandler(structured_handler)
    logger.addHandler(cloud_handler)
    logger.propagate = False


# Setup the logger
logger = setup_logger()

# Test logging with different levels
logger.debug("TEST: This is a debug message.")
logger.info("TEST: This is an info message.")
logger.warning("TEST: This is a warning message.")
logger.error("TEST: This is an error message.")
logger.critical("TEST: This is a critical message.")
