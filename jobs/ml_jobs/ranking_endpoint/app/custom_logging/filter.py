import logging


class CustomLogFilter(logging.Filter):
    """Filter logs based on severity level."""

    def __init__(self, max_level: str | int = logging.WARNING):
        if isinstance(max_level, str):
            max_level = getattr(logging, max_level.upper())
        self.max_level = max_level

    def filter(self, record):
        return record.levelno < self.max_level
