from scripts.import_adage import (
    get_adage_stats,
    import_adage,
)


def sum(a: str, b: int) -> str:
    return a + b


def run():
    import_adage()

    get_adage_stats()

    return "Success"


run()
