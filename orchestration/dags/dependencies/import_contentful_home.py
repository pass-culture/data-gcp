import pandas as pd
import pandas_gbq as gbq
from common.config import (
    DATA_GCS_BUCKET_NAME,
    GCP_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
)


def create_table_contentful_home():
    return f"""CREATE TABLE IF NOT EXISTS {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.contentful_home (
                Home STRING, 
                EntryID STRING, )
    """


def load_from_csv_contentful_file():
    data = pd.read_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/Contentful_home/contentful_home.csv", sep=";"
    )
    data.to_gbq(
        f"""{BIGQUERY_ANALYTICS_DATASET}.contentful_home""",
        project_id=f"{GCP_PROJECT}",
        if_exists="replace",
    )
    return
