import time
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def log_duration(message, start):
    logging.info(f"{message}: {time.time() - start} seconds.")
