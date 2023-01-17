from scripts.import_adage import (
    create_adage_table,
    get_request,
    get_adage_stats,
    adding_value,
    ENDPOINT,
    API_KEY,
)
from google.cloud import bigquery
from scripts.utils import GCP_PROJECT, BIGQUERY_TMP_DATASET


def run(request):
    """The Cloud Function entrypoint."""
    client = bigquery.Client()
    client.query(create_adage_table()).result()
    df = get_request(ENDPOINT, API_KEY, route="partenaire-culturel")
    df.to_gbq(
        f"""{BIGQUERY_TMP_DATASET}.adage_data_temp""",
        project_id=GCP_PROJECT,
        if_exists="replace",
    )
    client.query(adding_value()).result()

    get_adage_stats()

    return "Success"
