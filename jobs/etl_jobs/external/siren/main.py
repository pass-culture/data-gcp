from scripts.import_siren import siren_to_bq


def run(request):
    """The Cloud Function entrypoint."""
    siren_to_bq()

    return "Success"
