import logging

from scripts.import_adage import (
    get_adage_stats,
    import_adage,
)


def run():
    try:
        import_adage()
        get_adage_stats()
        return "Success"
    except Exception:
        return "Failed"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    logging.info(f"Import adage task {result}")
