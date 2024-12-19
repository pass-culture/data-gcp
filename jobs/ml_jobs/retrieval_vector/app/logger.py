import logging

import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging.handlers.transports import BackgroundThreadTransport


def setup_logger():
    # Create the Google Cloud Logging client
    client = google.cloud.logging.Client()

    # Create a Cloud Logging handler
    cloud_handler = CloudLoggingHandler(client, transport=BackgroundThreadTransport)

    # Set up a standard Python logger
    logger = logging.getLogger("hypercorn")
    logger.setLevel(logging.INFO)  # Set base level to INFO
    logger.addHandler(cloud_handler)

    # Add a console handler for local debugging (optional)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()

logger.debug("TEST: This is a debug message.")
logger.info("TEST: This is an info message.")
logger.warning("TEST: This is a warning message.")
logger.error("This TEST: is an error message.")
