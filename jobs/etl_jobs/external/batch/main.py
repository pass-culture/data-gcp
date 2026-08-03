import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_gbq
import typer

from batch_client import BatchClient
from utils import access_secret_data, bigquery_load_job

logger = logging.getLogger(__name__)


def main(
    gcp_project_id,
    env_short_name,
    operating_system,
):
    try:
        if operating_system == "android":
            API_KEY = access_secret_data(
                gcp_project_id, f"batch-android-api-key-{env_short_name}", version_id=1
            )
        elif operating_system == "ios":
            API_KEY = access_secret_data(
                gcp_project_id, f"batch-ios-api-key-{env_short_name}", version_id=1
            )
        else:
            raise RuntimeError(
                "You need to provide an operating system supported by Batch: ios|android."
            )

        REST_API_KEY = access_secret_data(
            gcp_project_id, f"batch-rest-api-key-{env_short_name}", version_id=1
        )

        batch_client = BatchClient(
            API_KEY, REST_API_KEY, operating_system=operating_system
        )

        # Campaigns
        metadata = batch_client.get_campaigns_metadata()
        pandas_gbq.to_gbq(
            metadata,
            destination_table=f"raw_{env_short_name}.batch_campaigns_ref",
            if_exists="replace",
        )

        campaigns_stats_df = batch_client.get_campaigns_stats()
        if "versions" in campaigns_stats_df.columns:
            ab_testing_df = batch_client.get_ab_testing_details(campaigns_stats_df)
            stats = batch_client.get_campaigns_stats_detailed(
                campaigns_stats_df, ab_testing_df
            )
        else:
            stats = campaigns_stats_df
            stats["version"] = np.nan
        stats = stats.assign(operating_system=operating_system)
        pandas_gbq.to_gbq(
            stats,
            destination_table=f"raw_{env_short_name}.batch_campaigns_stats",
            if_exists="append",
        )

        # Transactional
        if env_short_name == "prod":
            transactional_group_ids = [
                "Cancel_booking",
                "Offer_link",
                "Soon_expiring_bookings",
                "Today_stock",
                "Favorites_not_booked",
            ]
            start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
            end_date = datetime.today().strftime("%Y-%m-%d")
        elif env_short_name == "dev":
            transactional_group_ids = [
                "Offer_link",
                "Cancel_booking",
                "Today_stock",
            ]
            start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
            end_date = datetime.today().strftime("%Y-%m-%d")
        else:
            transactional_group_ids = [
                "Cancel_booking",
                "Soon_expiring_bookings",
                "Today_stock",
                "Offer_link",
            ]
            start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
            end_date = datetime.today().strftime("%Y-%m-%d")

        transac_dfs = []

        for group_id in transactional_group_ids:
            print(group_id)
            df = batch_client.get_transactional_stats(group_id, start_date, end_date)
            transac_dfs.append(df)

        transac_df = pd.concat(transac_dfs).assign(operating_system=operating_system)

        print(transac_df.head())

        bigquery_load_job(
            df=transac_df,
            partition_date=datetime.strptime(end_date, "%Y-%m-%d"),
            partitioning_field="update_date",
            gcp_project_id=gcp_project_id,
            dataset=f"raw_{env_short_name}",
            table_name="batch_transac",
            schema={
                "date": "STRING",
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
    except typer.Exit:
        raise
    except Exception as e:
        logger.exception(f"ETL job failed: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    print("Run Batch !")
    typer.run(main)
