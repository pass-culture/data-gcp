import logging

import google.cloud.logging


class CustomLogger:
    def __init__(self, logger=None):
        self.logger = logger

    def info(
        self,
        message=None,
    ):
        self.logger.info(message)

    def warn(
        self,
        message=None,
    ):
        self.logger.warning(message)

    def debug(
        self,
        message=None,
    ):
        self.logger.debug(message)

    def error(
        self,
        message=None,
    ):
        self.logger.error(message)


def get_vertexai_logger() -> logging.Logger:
    # """
    # Configures and returns a logger suitable for Vertex AI applications.

    # Returns:
    #     logging.Logger: A configured logger.
    # """
    # logger = logging.getLogger("vertexai-logger")
    # logger.setLevel(logging.DEBUG)  # Capture all log levels (DEBUG and above)

    # # Create a StreamHandler to output logs to stdout
    # stdout_handler = logging.StreamHandler(stream=sys.stdout)

    # # Apply the VertexAIJsonFormatter to format logs in JSON
    # formatter = VertexAIJsonFormatter(
    #     "%(timestamp)s %(severity)s %(name)s %(message)s",
    #     rename_fields={"asctime": "timestamp", "levelname": "severity"},
    # )
    # stdout_handler.setFormatter(formatter)

    # # Attach the handler to the logger
    # logger.addHandler(stdout_handler)
    client = google.cloud.logging.Client()
    handler = client.get_default_handler()
    handler.setLevel(logging.INFO)
    handler.filters = []
    api_logger = logging.getLogger("vertexai-logger")
    api_logger.handlers = []
    api_logger.addHandler(handler)
    api_logger.setLevel(logging.DEBUG)

    return CustomLogger(logger=api_logger)


# Example usage
logger = get_vertexai_logger()

logger.debug("This is a debug message.")
logger.info("This is an info message.")
logger.warning("This is a warning message.")
logger.error("This is an error message.")
logger.critical("This is a critical message.")
