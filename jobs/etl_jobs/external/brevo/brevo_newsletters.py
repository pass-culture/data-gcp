import brevo_python
import pandas as pd
from brevo_python.rest import ApiException
from google.cloud import bigquery


class BrevoNewsletters:
    def __init__(
        self,
        gcp_project,
        raw_dataset,
        api_key,
        destination_table_name,
        start_date,
        end_date,
    ):
        self.gcp_project = gcp_project
        self.raw_dataset = raw_dataset
        self.destination_table_name = destination_table_name
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date

    def create_instance_email_campaigns_api(self):
        configuration = brevo_python.Configuration()
        configuration.api_key["api-key"] = self.api_key  # get secret

        api_instance = brevo_python.EmailCampaignsApi(
            brevo_python.ApiClient(configuration)
        )

        self.api_instance = api_instance

    def get_email_campaigns(self):
        campaigns_list = []
        try:
            campaigns = self.api_instance.get_email_campaigns(
                status="sent",
                limit=50,
                statistics="globalStats",
            )
            campaigns_list = campaigns.campaigns
        except ApiException as e:
            print(
                "Exception when calling EmailCampaignsApi->get_email_campaigns: %s\n"
                % e
            )

        return campaigns_list

    def get_data(self):
        campaigns_list = self.get_email_campaigns()

        campaign_stats = {}
        campaign_stats["campaign_id"] = [camp.get("id") for camp in campaigns_list]
        campaign_stats["campaign_utm"] = [camp.get("tag") for camp in campaigns_list]
        campaign_stats["campaign_name"] = [camp.get("name") for camp in campaigns_list]
        campaign_stats["campaign_sent_date"] = [
            camp.get("sentDate") for camp in campaigns_list
        ]
        campaign_stats["share_link"] = [
            camp.get("shareLink") for camp in campaigns_list
        ]
        campaign_stats["audience_size"] = [
            group.get("globalStats").get("delivered")
            if len(group.get("globalStats")) > 0
            else 0
            for group in [camp.get("statistics") for camp in campaigns_list]
        ]
        campaign_stats["unsubscriptions"] = [
            group.get("globalStats").get("unsubscriptions")
            if len(group.get("globalStats")) > 0
            else 0
            for group in [camp.get("statistics") for camp in campaigns_list]
        ]
        campaign_stats["open_number"] = [
            group.get("globalStats").get("uniqueViews")
            if len(group.get("globalStats")) > 0
            else 0
            for group in [camp.get("statistics") for camp in campaigns_list]
        ]

        campaign_stats_df = (
            pd.DataFrame(campaign_stats)
            .set_index(
                [
                    "campaign_id",
                    "campaign_utm",
                    "campaign_name",
                    "campaign_sent_date",
                    "share_link",
                ]
            )
            .reset_index()
            .assign(
                update_date=pd.to_datetime("today"),
            )[
                [
                    "campaign_id",
                    "campaign_utm",
                    "campaign_name",
                    "campaign_sent_date",
                    "share_link",
                    "audience_size",
                    "open_number",
                    "unsubscriptions",
                    "update_date",
                ]
            ]
        )

        return campaign_stats_df

    def save_to_historical(self, df_to_save, schema):
        bigquery_client = bigquery.Client()

        _now = self.end_date
        yyyymmdd = _now.strftime("%Y%m%d")
        table_id = f"{self.gcp_project}.{self.raw_dataset}.{self.destination_table_name}_histo${yyyymmdd}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="update_date"
            ),
            schema=[
                bigquery.SchemaField(column, _type) for column, _type in schema.items()
            ],
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
        )
        job = bigquery_client.load_table_from_dataframe(
            df_to_save, table_id, job_config=job_config
        )
        job.result()
