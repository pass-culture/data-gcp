import logging

from scripts.import_adage import (
    get_adage_stats,
    import_adage,
)


def run():
    import_adage()
    get_adage_stats()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    run()
    logger.info(" Adage import task completed successfully ")
