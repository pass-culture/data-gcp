import logging
from datetime import datetime, timedelta

from scripts.import_adage import (
    get_adage_stats,
    import_adage,
)


def run():
    since_date = datetime.now() - timedelta(days=30)
    import_adage(since_date)
    get_adage_stats()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    run()
    logger.info("Adage import task completed successfully")
