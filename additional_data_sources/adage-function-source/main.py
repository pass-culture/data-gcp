from scripts.import_adage import (
    save_adage_to_bq,
    create_adage_table,
    get_partenaire_culturel,
)
from google.cloud import bigquery


def run(request):
    """The Cloud Function entrypoint."""
    client = bigquery.Client()
    client.query(create_adage_table()).result()
    datas = get_partenaire_culturel()
    pd.DataFrame(datas).to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.adage_data_temp""",
        project_id=GCP_PROJECT,
        if_exists="replace",
    )
    client.query(adding_value()).result()
    return "Success"
