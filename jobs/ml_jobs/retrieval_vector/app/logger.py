import logging

import google.cloud.logging


def get_logger() -> logging.Logger:
    """Configure and return the logger with automatic Google Cloud Logging setup."""
    # Instantiates a Cloud Logging client
    client = google.cloud.logging.Client()

    # Automatically configures Cloud Logging with the standard Python logging module
    client.setup_logging()

    # Get the root logger
    logger = logging.getLogger()

    # Set the logging level if needed (default is INFO)
    logger.setLevel(logging.INFO)

    return logger


# Get the logger instance
logger = get_logger()

# Example usage
logger.info("This is an INFO log.")
logger.error("This is an ERROR log.")
