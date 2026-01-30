import logging
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def log_duration(message: str, start: float):
    duration_in_minutes = (time.time() - start) / 60
    logging.info(f"{message}: {duration_in_minutes} minutes.")
