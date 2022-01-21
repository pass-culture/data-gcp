from scripts.import_adage import save_adage_to_bq


def run(request):
    """The Cloud Function entrypoint."""
    save_adage_to_bq()
    return "Success"
