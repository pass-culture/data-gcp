import pandas as pd
import pandas_gbq

from qualtrics_client import QualtricsClient
from utils import BIGQUERY_RAW_DATASET, PROJECT_NAME


def import_qualtrics_opt_out(
    client: QualtricsClient, directory_id: str, export_columns: dict
) -> None:
    results = client.fetch_opt_out_contacts(directory_id)
    df = pd.DataFrame(results).rename(columns=export_columns)
    df["directory_unsubscribe_date"] = pd.to_datetime(df["directory_unsubscribe_date"])
    pandas_gbq.to_gbq(
        df[["directory_unsubscribe_date", "contact_id", "email", "ext_ref"]],
        f"{BIGQUERY_RAW_DATASET}.qualtrics_opt_out_users",
        project_id=PROJECT_NAME,
        if_exists="replace",
    )
