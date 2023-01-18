from scripts.import_adage import (
    get_adage_stats,
    import_adage,
)


def run(request):
    """The Cloud Function entrypoint."""
    import_adage()

    get_adage_stats()

    return "Success"
