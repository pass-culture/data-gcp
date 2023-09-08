from scripts.import_adage import (
    get_adage_stats,
    import_adage,
)


def run():
    import_adage()

    get_adage_stats()

    return "Success"


run()
