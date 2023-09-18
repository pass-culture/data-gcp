from scripts.import_siren import siren_to_bq


def run():
    """The Cloud Function entrypoint."""
    siren_to_bq()

    return "Success"


run()
