from scripts.import_siret import siret_to_bq


def run():
    """The Cloud Function entrypoint."""
    siret_to_bq()

    return "Success"


run()
