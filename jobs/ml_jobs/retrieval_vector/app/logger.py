import logging

# Imports the Cloud Logging client library
import google.cloud.logging


def setup_logger():
    # Instantiates a client
    client = google.cloud.logging.Client()

    # Retrieves a Cloud Logging handler based on the environment
    # you're running in and integrates the handler with the
    # Python logging module. By default this captures all logs
    # at INFO level and higher
    client.setup_logging()

    # Set up a standard Python logger
    logger = logging.getLogger("hypercorn")
    logger.setLevel(logging.INFO)  # Set base level to INFO

    return logger


# Setup the logger
logger = setup_logger()

# Test logging with different levels
logger.debug("TEST: This is a debug message.")
logger.info("TEST: This is an info message.")
logger.warning("TEST: This is a warning message.")
logger.error("TEST: This is an error message.")
logger.critical("TEST: This is a critical message.")
