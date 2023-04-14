import requests
import typer
import pandas as pd
from google.cloud import bigquery
import argparse

from utils import access_secret_data, bigquery_load_job, GCP_PROJECT_ID, ENV_SHORT_NAME
from batch_client import BatchClient


def main(
    gcp_project_id,
    env_short_name,
    operating_system,
):

    if operating_system == "android":
        API_KEY = access_secret_data(
            gcp_project_id, f"batch-android-api-key-{env_short_name}", version_id=1
        )
        REST_API_KEY = access_secret_data(
            gcp_project_id, f"batch-android-rest-api-key-{env_short_name}", version_id=1
        )
    elif operating_system == "ios":
        API_KEY = access_secret_data(
            gcp_project_id, f"batch-ios-api-key-{env_short_name}", version_id=1
        )
        REST_API_KEY = access_secret_data(
            gcp_project_id, f"batch-ios-rest-api-key-{env_short_name}", version_id=1
        )
    else:
        raise RuntimeError(
            "You need to provide an operating system supported by Batch: ios|android."
        )

    batch_client = BatchClient(API_KEY, REST_API_KEY)
    bigquery_client = bigquery.Client()

    # Campaigns
    metadata = batch_client.get_campaigns_metadata()
    metadata.to_gbq(
        destination_table="{{bigquery_raw_dataset}}.batch_campaigns_ref",
        if_exists="append",
    )

    campaigns_stats_df = batch_client.get_campaigns_stats()
    ab_testing_df = batch_client.get_ab_testing_details(campaigns_stats_df)
    stats = batch_client.get_campaigns_stats_detailed(campaigns_stats_df, ab_testing_df)
    stats = stats.assign(operating_system=operating_system)
    stats.to_gbq(
        destination_table="{{bigquery_raw_dataset}}.batch_campaigns_stats",
        if_exists="append",
    )

    # Transactional
    transactional_group_ids = [
        "Cancel_booking",
        "Offer_link",
        "Soon_expiring_bookings",
        "Today_stock",
        "Favorites_not_booked",
    ]
    transac_dfs = []

    for group_id in transactional_group_ids:
        df = batch_client.get_transactional_stats(group_id, start_date, end_date)
        transac_dfs.append(df)

    transac_df = pd.concat(transac_dfs).assign(operating_system=operating_system)

    bigquery_load_job(
        df=transac_df,
        partition_date=end_date,
        partitioning_field="update_date",
        gcp_project_id=gcp_project_id,
        dataset=bq_raw_dataset,
        table_name="batch_transac",
        schema={
            "date": "DATE",
            "sent": "INTEGER",
            "direct_open": "INTEGER",
            "influenced_open": "INTEGER",
            "reengaged": "INTEGER",
            "errors": "INTEGER",
            "group_id": "STRING",
            "update_date": "DATE",
            "operating_system": "STRING",
        },
    )

if __name__ == "__main__":
    typer.run(main)