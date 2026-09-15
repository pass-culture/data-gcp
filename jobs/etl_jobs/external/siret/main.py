from scripts.import_siret import siret_to_bq


def run():
    siret_to_bq()

    return "Success"


run()
