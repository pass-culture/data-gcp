import logging
import sys

from pythonjsonlogger import jsonlogger

STDERR_LOG_LEVEL = logging.WARNING
STDOUT_LOG_LEVEL = logging.DEBUG


class CustomLogFilter(logging.Filter):
    """Filter logs based on severity level."""

    def __init__(self, max_level):
        self.max_level = max_level

    def filter(self, record):
        return record.levelno < self.max_level


def setup_logging(stdout_log_level: int, stderr_log_level: int) -> None:
    """
    Configure logging for the entire application, including Hypercorn.
        - We use this kind of logging because vertexAI online prediction logging only allow ERROR and INFO logs.
        - All logs writtent to stdout are mapped to INFO level.
        - All logs written to stderr are mapped to ERROR level.
        Ref : see https://cloud.google.com/vertex-ai/docs/predictions/online-prediction-logging?hl=en

    Args:
        stdout_log_level: The log level for stdout logs.
        stderr_log_level: The log level for stderr.
    """

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(stdout_log_level)

    # Formatter
    formatter = jsonlogger.JsonFormatter(
        "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
        rename_fields={"levelname": "severity", "asctime": "timestamp"},
    )

    # Handlers
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(stdout_log_level)
    stdout_handler.addFilter(CustomLogFilter(stderr_log_level))
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(STDERR_LOG_LEVEL)
    stderr_handler.setFormatter(formatter)

    # Apply handlers to root logger
    root_logger.handlers = []
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)


setup_logging(stdout_log_level=STDOUT_LOG_LEVEL, stderr_log_level=STDERR_LOG_LEVEL)

logger = logging.getLogger("api_logger")
