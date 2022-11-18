import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

import pandas as pd
from datetime import datetime, timedelta, timezone
import time

from google.cloud import bigquery

from utils import ENV_SHORT_NAME


class sendinblue_email_campaigns:
    def __init__(self, gcp_project, raw_dataset, api_key, destination_table_name):

        self.gcp_project = gcp_project
        self.raw_dataset = raw_dataset
        self.destination_table_name = destination_table_name
        self.api_key = api_key

    def create_instance_email_campaigns_api(self):

        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key["api-key"] = self.api_key  # get secret

        api_instance = sib_api_v3_sdk.EmailCampaignsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )

        self.api_instance = api_instance

    def get_email_campaigns(self):

        api_instance = self.api_instance

        try:
            campaigns = api_instance.get_email_campaigns(status="sent", limit=50)
            campaigns_list = campaigns.campaigns
        except ApiException as e:
            print(
                "Exception when calling EmailCampaignsApi->get_email_campaigns: %s\n"
                % e
            )

        return campaigns_list

    def get_email_campaigns_to_update(self):
        """Get emails camapaigns ids sent in the last 31 days."""

        campaigns_list = self.get_email_campaigns()

        if ENV_SHORT_NAME == "dev":
            window = 500
        else:
            window = 31

        campaigns_to_update_list = [
            camp.get("id")
            for camp in campaigns_list
            if camp.get("sentDate") is not None
            and datetime.strptime(camp.get("sentDate"), "%Y-%m-%dT%H:%M:%S.%f%z")
            >= datetime.now(tz=timezone.utc) - timedelta(days=window)
        ]

        print("id campaign to update ---- ", campaigns_to_update_list)
        return campaigns_to_update_list

    def get_data_by_domain(self):

        campaigns_list = self.get_email_campaigns()
        campaigns_to_update_list = self.get_email_campaigns_to_update()

        campaign_stats_by_domain = {}
        campaign_stats_by_domain["campaign_id"] = [
            camp.get("id")
            for camp in campaigns_list
            if camp.get("id") in campaigns_to_update_list
        ]
        campaign_stats_by_domain["campaign_name"] = [
            camp.get("name")
            for camp in campaigns_list
            if camp.get("id") in campaigns_to_update_list
        ]
        campaign_stats_by_domain["campaign_sent_date"] = [
            camp.get("sentDate")
            for camp in campaigns_list
            if camp.get("id") in campaigns_to_update_list
        ]
        campaign_stats_by_domain["share_link"] = [
            camp.get("shareLink")
            for camp in campaigns_list
            if camp.get("id") in campaigns_to_update_list
        ]
        campaign_stats_by_domain["domain"] = [
            list(group.get("statsByDomain").keys())
            for group in [
                camp.get("statistics")
                for camp in campaigns_list
                if camp.get("id") in campaigns_to_update_list
            ]
        ]

        domain_stats_df = pd.DataFrame()
        for campaign_id in campaigns_to_update_list:
            temp = (
                pd.DataFrame(
                    [
                        group.get("statsByDomain")
                        for group in [
                            camp.get("statistics")
                            for camp in campaigns_list
                            if camp.get("id") == campaign_id
                        ]
                    ][0]
                )
                .transpose()
                .reset_index()
                .rename(columns={"index": "domain"})
            )

            temp["campaign_id"] = [
                camp.get("id")
                for camp in campaigns_list
                if camp.get("id") == campaign_id
            ][0]
            domain_stats_df = pd.concat([temp, domain_stats_df])

        print("print dict ----", campaign_stats_by_domain)
        campaign_stats_by_domain_df = (
            pd.DataFrame(campaign_stats_by_domain)
            .set_index(
                ["campaign_id", "campaign_name", "campaign_sent_date", "share_link"]
            )
            .explode("domain")
            .reset_index()
            .merge(domain_stats_df, on=["campaign_id", "domain"])
            .rename(columns={"sent": "audience_size", "uniqueViews": "open_number"})
            .assign(update_date=pd.to_datetime("today"))[
                [
                    "campaign_id",
                    "campaign_name",
                    "campaign_sent_date",
                    "share_link",
                    "domain",
                    "audience_size",
                    "open_number",
                    "unsubscriptions",
                    "update_date",
                ]
            ]
        )

        return campaign_stats_by_domain_df

    def save_to_raw(self, df_to_save):

        df_to_save.to_gbq(
            destination_table=f"{self.gcp_project}.{self.raw_dataset}.{self.destination_table_name}",
            project_id=self.gcp_project,
            if_exists="replace",
        )

    def save_to_historical(self, df_to_save):

        bigquery_client = bigquery.Client()

        _now = datetime.today()
        yyyymmdd = _now.strftime("%Y%m%d")
        table_id = f"{self.gcp_project}.{self.raw_dataset}.{self.destination_table_name}_histo${yyyymmdd}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="update_date",
            ),
        )
        job = bigquery_client.load_table_from_dataframe(
            df_to_save, table_id, job_config=job_config
        )
        job.result()
