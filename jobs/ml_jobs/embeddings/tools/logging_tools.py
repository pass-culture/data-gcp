import time

import psutil
from loguru import logger


def log_memory_info():
    return psutil.virtual_memory()._asdict()


def log_duration(message, start):
    logger.info(f"{message}: {time.time() - start} seconds.")
