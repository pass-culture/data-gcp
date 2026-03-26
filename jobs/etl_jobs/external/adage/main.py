import logging
from datetime import datetime, timedelta

from scripts.import_adage import (
    get_adage_stats,
    import_adage,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger(__name__)


def run():
    since_date = datetime.now() - timedelta(days=30)
    logger.info("Starting Adage import pipeline (since_date=%s)", since_date.date())
    import_adage(since_date)
    logger.info("Adage partner import completed, starting stats collection")
    get_adage_stats()


if __name__ == "__main__":
    run()
    logger.info("Adage import task completed successfully")
