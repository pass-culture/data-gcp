import logging

from google.cloud import logging as cloud_logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from google.cloud.logging_v2.handlers.transports import BackgroundThreadTransport


class CustomCloudLoggingHandler(CloudLoggingHandler):
    """Custom Cloud Logging handler to ensure severity is set correctly."""

    def emit(self, record):
        # Map Python log levels to Google Cloud severity levels
        severity_map = {
            logging.DEBUG: "DEBUG",
            logging.INFO: "INFO",
            logging.WARNING: "WARNING",
            logging.ERROR: "ERROR",
            logging.CRITICAL: "CRITICAL",
        }

        # Set the severity on the record
        record.severity = severity_map.get(record.levelno, "DEFAULT")
        super().emit(record)


def setup_logger():
    # Create the Google Cloud Logging client
    client = cloud_logging.Client()

    # Use the custom Cloud Logging handler
    cloud_handler = CustomCloudLoggingHandler(
        client, transport=BackgroundThreadTransport
    )

    # Set up a standard Python logger
    logger = logging.getLogger("hypercorn")
    logger.setLevel(logging.INFO)  # Set base level to INFO
    logger.addHandler(cloud_handler)

    # Add a console handler for local debugging (optional)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console_handler)

    return logger


# Setup the logger
logger = setup_logger()

# Test logging with different levels
logger.debug("TEST: This is a debug message.")
logger.info("TEST: This is an info message.")
logger.warning("TEST: This is a warning message.")
logger.error("TEST: This is an error message.")
logger.critical("TEST: This is a critical message.")
