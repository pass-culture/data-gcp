import logging
import sys

from pythonjsonlogger import jsonlogger


class VertexAIJsonFormatter(jsonlogger.JsonFormatter):
    def process_log_record(self, log_record):
        """
        Ensures that the `severity` field aligns with the logger's log level
        and is placed in the format Google Cloud Logging expects.
        """
        # Map `levelname` to `severity` for Google Cloud compatibility
        log_record["severity"] = log_record.get("levelname", "DEFAULT")
        return log_record


def get_vertexai_logger() -> logging.Logger:
    """
    Configures and returns a logger suitable for Vertex AI applications.

    Returns:
        logging.Logger: A configured logger.
    """
    logger = logging.getLogger("vertexai-logger")
    logger.setLevel(logging.DEBUG)  # Capture all log levels (DEBUG and above)

    # Create a StreamHandler to output logs to stdout
    stdout_handler = logging.StreamHandler(stream=sys.stdout)

    # Apply the VertexAIJsonFormatter to format logs in JSON
    formatter = VertexAIJsonFormatter(
        "%(timestamp)s %(severity)s %(name)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "severity"},
    )
    stdout_handler.setFormatter(formatter)

    # Attach the handler to the logger
    logger.addHandler(stdout_handler)

    return logger


# Example usage
logger = get_vertexai_logger()

logger.debug("This is a debug message.")
logger.info("This is an info message.")
logger.warning("This is a warning message.")
logger.error("This is an error message.")
logger.critical("This is a critical message.")
