from scripts.import_adage import (
    create_adage_table,
    get_partenaire_culturel,
    adding_value,
    ENDPOINT,
    API_KEY,
)
from google.cloud import bigquery
import pandas as pd
from scripts.utils import GCP_PROJECT, BIGQUERY_ANALYTICS_DATASET


def run(request):
    """The Cloud Function entrypoint."""
    client = bigquery.Client()
    client.query(create_adage_table()).result()
    datas = get_partenaire_culturel(ENDPOINT, API_KEY)
    pd.DataFrame(datas).to_gbq(
        f"""{BIGQUERY_ANALYTICS_DATASET}.adage_data_temp""",
        project_id=GCP_PROJECT,
        if_exists="replace",
    )
    client.query(adding_value()).result()
    return "Success"
