from scripts.import_adage import (
    save_adage_to_bq,
    create_adage_table,
    get_partenaire_culturel,
)
from google.cloud import bigquery


def run():
    """The Cloud Function entrypoint."""
    client = bigquery.Client()
    client.query(create_adage_table()).result()
    datas = get_partenaire_culturel()
    for i in range(len(datas)):
        client.query(save_adage_to_bq(datas, i)).result()
    return "Success"
