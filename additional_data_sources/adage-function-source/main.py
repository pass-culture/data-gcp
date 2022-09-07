from scripts.import_adage import (
    create_adage_table,
    get_request,
    adding_value,
    ENDPOINT,
    API_KEY,
)
from google.cloud import bigquery
import pandas as pd
from scripts.utils import GCP_PROJECT, BIGQUERY_ANALYTICS_DATASET, save_to_raw_bq
from datetime import datetime


def run(request):
    """The Cloud Function entrypoint."""
    client = bigquery.Client()
    client.query(create_adage_table()).result()
    datas = get_request(ENDPOINT, API_KEY, route="partenaire-culturel")
    pd.DataFrame(datas).to_gbq(
        f"""{BIGQUERY_ANALYTICS_DATASET}.adage_data_temp""",
        project_id=GCP_PROJECT,
        if_exists="replace",
    )
    client.query(adding_value()).result()

    adage_ids = [7, 8]

    export = []
    for _id in adage_ids:

        results = get_request(ENDPOINT, API_KEY, route=f"stats-pass-culture/{_id}")

        for k, v in results.items():
            export.append(
                dict(
                    {
                        "academie_id": k,
                        "adage_id": _id,
                    },
                    **v,
                )
            )

    df = pd.DataFrame(export)
    save_to_raw_bq(df, "adage_eple_stats")

    return "Success"
