import logging
import sys

from pythonjsonlogger import jsonlogger


class CustomLogFilter(logging.Filter):
    """Filter logs based on severity level."""

    def __init__(self, max_level):
        self.max_level = max_level

    def filter(self, record):
        return record.levelno < self.max_level


def setup_logging():
    """Configure logging for the entire application, including Flask, Werkzeug, and Hypercorn."""
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Formatter
    formatter = jsonlogger.JsonFormatter(
        "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
        rename_fields={"levelname": "severity", "asctime": "timestamp"},
    )

    # Handlers
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(CustomLogFilter(logging.WARNING))
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(formatter)

    # Apply handlers to root logger
    root_logger.handlers = []
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)


setup_logging()

# Example logs
logger = logging.getLogger("api_logger")
logger.debug("TEST: Debug message")
logger.info("TEST: Info message")
logger.warning("TEST: Warning message")
logger.error("TEST: Error message")
