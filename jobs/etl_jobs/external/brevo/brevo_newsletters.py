import logging
from datetime import datetime

import brevo_python
import pandas as pd
from brevo_python.rest import ApiException
from google.cloud import bigquery

from utils import (
    ENV_SHORT_NAME,
    rate_limiter,
)


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

    def check_rate_limit_status(self):
        """Check current rate limit status before starting."""
        try:
            # Make a lightweight API call to check status
            self.api_instance.get_email_campaigns(status="sent", limit=1, offset=0)
            logging.info("Rate limit check: Newsletter API is accessible")
            return True
        except ApiException as e:
            if e.status == 429:
                if hasattr(e, "http_resp") and hasattr(e.http_resp, "headers"):
                    headers = e.http_resp.headers
                    reset_time = headers.get("x-sib-ratelimit-reset")
                    remaining = headers.get("x-sib-ratelimit-remaining")
                    limit = headers.get("x-sib-ratelimit-limit")

                    logging.warning("Newsletter API rate limit exceeded!")
                    logging.warning(f"  Limit: {limit}, Remaining: {remaining}")
                    if reset_time:
                        logging.warning(
                            f"  Reset in: {reset_time} seconds ({int(reset_time)/60:.1f} minutes)"
                        )
                    else:
                        logging.warning("  Reset time unknown")
                    return False
                else:
                    logging.warning(
                        "Newsletter API rate limit exceeded (no details available)"
                    )
                    return False
            raise

    # Use the actual Brevo API limit with safety margin
    @rate_limiter(
        calls=280, period=3600, log_waits=True
    )  # 280 calls per hour (safe margin from 300)
    def _get_email_campaigns(self, **kwargs):
        return self.api_instance.get_email_campaigns(**kwargs)

    def get_email_campaigns(self):
        # Check rate limit status before starting
        if not self.check_rate_limit_status():
            logging.error(
                "Newsletter API rate limit exceeded. Please wait before retrying."
            )
            return []

        campaigns_list = []
        try:
            offset = 0
            limit = 50
            total_campaigns = 0
            total_calls = 0

            logging.info("Fetching email campaigns...")

            # First call to get initial campaigns
            response = self._get_email_campaigns(
                status="sent",
                limit=limit,
                offset=offset,
                statistics="globalStats",
                start_date=self.start_date.strftime("%Y-%m-%d")
                if hasattr(self.start_date, "strftime")
                else self.start_date,
                end_date=self.end_date.strftime("%Y-%m-%d")
                if hasattr(self.end_date, "strftime")
                else self.end_date,
            )
            total_calls += 1

            if response and response.campaigns:
                campaigns_list.extend(response.campaigns)
                total_campaigns = len(response.campaigns)
                logging.info(f"Initial batch: {len(response.campaigns)} campaigns")

                # Handle pagination for production environment
                if ENV_SHORT_NAME == "prod":
                    # Continue fetching if we got a full page
                    while (
                        response
                        and response.campaigns
                        and len(response.campaigns) == limit
                    ):
                        offset += limit
                        logging.info(f"Fetching campaigns at offset {offset}...")

                        try:
                            response = self._get_email_campaigns(
                                status="sent",
                                limit=limit,
                                offset=offset,
                                statistics="globalStats",
                                start_date=self.start_date.strftime("%Y-%m-%d")
                                if hasattr(self.start_date, "strftime")
                                else self.start_date,
                                end_date=self.end_date.strftime("%Y-%m-%d")
                                if hasattr(self.end_date, "strftime")
                                else self.end_date,
                            )
                            total_calls += 1

                            if response and response.campaigns:
                                campaigns_list.extend(response.campaigns)
                                total_campaigns += len(response.campaigns)
                                logging.info(
                                    f"Batch at offset {offset}: {len(response.campaigns)} campaigns"
                                )
                            else:
                                break

                        except ApiException as e:
                            if e.status == 429:
                                logging.error(
                                    f"Rate limit exceeded while fetching campaigns at offset {offset}"
                                )
                                logging.error(
                                    "Stopping pagination to avoid further rate limit issues"
                                )
                                break
                            else:
                                logging.error(f"API error at offset {offset}: {e}")
                                break
                        except Exception as e:
                            logging.error(f"Unexpected error at offset {offset}: {e}")
                            break

            logging.info(f"Total campaigns fetched: {total_campaigns}")
            logging.info(f"Total API calls made: {total_calls}")

        except ApiException as e:
            if e.status == 429:
                logging.error("Rate limit exceeded while fetching initial campaigns")
                if hasattr(e, "http_resp") and hasattr(e.http_resp, "headers"):
                    headers = e.http_resp.headers
                    reset_time = headers.get("x-sib-ratelimit-reset")
                    remaining = headers.get("x-sib-ratelimit-remaining")
                    limit = headers.get("x-sib-ratelimit-limit")

                    logging.error(f"  Limit: {limit}, Remaining: {remaining}")
                    if reset_time:
                        logging.error(
                            f"  Reset in: {reset_time} seconds ({int(reset_time)/60:.1f} minutes)"
                        )
                    else:
                        logging.error("  Reset time unknown")
                # Return empty list instead of crashing
                return []
            else:
                logging.error(
                    f"API error when calling EmailCampaignsApi->get_email_campaigns: {e}"
                )
                # Return empty list instead of crashing
                return []
        except Exception as e:
            logging.error(f"Unexpected error when fetching campaigns: {e}")
            return []

        return campaigns_list

    def safe_get_nested(self, obj, *keys, default=0):
        """Safely get nested dictionary values with fallback."""
        try:
            for key in keys:
                if obj is None:
                    return default
                obj = obj.get(key) if hasattr(obj, "get") else getattr(obj, key, None)
            return obj if obj is not None else default
        except (AttributeError, KeyError, TypeError):
            return default

    def get_data(self):
        campaigns_list = self.get_email_campaigns()

        if not campaigns_list:
            logging.warning("No campaigns found")
            # Return empty DataFrame with expected columns
            columns = [
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
            return pd.DataFrame(columns=columns)

        logging.info(f"Processing {len(campaigns_list)} campaigns...")

        # Safer data extraction with proper error handling
        campaign_stats = {}

        try:
            campaign_stats["campaign_id"] = [
                self.safe_get_nested(camp, "id", default="") for camp in campaigns_list
            ]
            campaign_stats["campaign_utm"] = [
                self.safe_get_nested(camp, "tag", default="") for camp in campaigns_list
            ]
            campaign_stats["campaign_name"] = [
                self.safe_get_nested(camp, "name", default="")
                for camp in campaigns_list
            ]
            campaign_stats["campaign_sent_date"] = [
                self.safe_get_nested(camp, "sentDate", default="")
                for camp in campaigns_list
            ]
            campaign_stats["share_link"] = [
                self.safe_get_nested(camp, "shareLink", default="")
                for camp in campaigns_list
            ]

            # Safer statistics extraction
            campaign_stats["audience_size"] = []
            campaign_stats["unsubscriptions"] = []
            campaign_stats["open_number"] = []

            for i, camp in enumerate(campaigns_list):
                if i % 10 == 0:  # Log progress every 10 campaigns
                    logging.info(f"Processing campaign {i+1}/{len(campaigns_list)}")

                stats = self.safe_get_nested(camp, "statistics")
                global_stats = (
                    self.safe_get_nested(stats, "globalStats") if stats else None
                )

                if global_stats and len(global_stats) > 0:
                    audience_size = self.safe_get_nested(
                        global_stats, "delivered", default=0
                    )
                    unsubscriptions = self.safe_get_nested(
                        global_stats, "unsubscriptions", default=0
                    )
                    open_number = self.safe_get_nested(
                        global_stats, "uniqueViews", default=0
                    )
                else:
                    audience_size = unsubscriptions = open_number = 0

                campaign_stats["audience_size"].append(audience_size)
                campaign_stats["unsubscriptions"].append(unsubscriptions)
                campaign_stats["open_number"].append(open_number)

            campaign_stats_df = pd.DataFrame(campaign_stats).assign(
                update_date=pd.to_datetime("today")
            )

            # Reorder columns to match expected output
            expected_columns = [
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

            # Ensure all expected columns exist
            for col in expected_columns:
                if col not in campaign_stats_df.columns:
                    campaign_stats_df[col] = ""

            campaign_stats_df = campaign_stats_df[expected_columns]

            logging.info(f"Successfully processed {len(campaign_stats_df)} campaigns")

            # Log some basic statistics
            if not campaign_stats_df.empty:
                total_delivered = campaign_stats_df["audience_size"].sum()
                total_opens = campaign_stats_df["open_number"].sum()
                total_unsubscribes = campaign_stats_df["unsubscriptions"].sum()

                logging.info("Campaign statistics:")
                logging.info(f"  Total emails delivered: {total_delivered}")
                logging.info(f"  Total opens: {total_opens}")
                logging.info(f"  Total unsubscribes: {total_unsubscribes}")
                if total_delivered > 0:
                    open_rate = (total_opens / total_delivered) * 100
                    logging.info(f"  Overall open rate: {open_rate:.2f}%")

            return campaign_stats_df

        except Exception as e:
            logging.error(f"Error processing campaign data: {e}")
            logging.info("Sample campaign data for debugging:")
            if campaigns_list:
                logging.info(
                    f"First campaign keys: {list(campaigns_list[0].keys()) if hasattr(campaigns_list[0], 'keys') else 'Not a dict'}"
                )
                # Log first campaign structure but limit output
                first_campaign = str(campaigns_list[0])[:500]  # Limit to 500 chars
                logging.info(f"First campaign (truncated): {first_campaign}...")
            raise

    def save_to_historical(self, df_to_save, schema):
        if df_to_save.empty:
            logging.warning("No campaign data to save")
            return

        bigquery_client = bigquery.Client()

        _now = self.end_date
        yyyymmdd = (
            _now.strftime("%Y%m%d")
            if hasattr(_now, "strftime")
            else datetime.now().strftime("%Y%m%d")
        )
        table_id = f"{self.gcp_project}.{self.raw_dataset}.{self.destination_table_name}_histo${yyyymmdd}"

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field="update_date"
                ),
                schema=[
                    bigquery.SchemaField(column, _type)
                    for column, _type in schema.items()
                ],
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ],
            )

            logging.info(f"Saving {len(df_to_save)} campaigns to BigQuery...")
            job = bigquery_client.load_table_from_dataframe(
                df_to_save, table_id, job_config=job_config
            )
            job.result()
            logging.info(
                f"Successfully saved {len(df_to_save)} campaigns to {table_id}"
            )

        except Exception as e:
            logging.error(f"Error saving to BigQuery: {e}")
            logging.info(f"Table ID: {table_id}")
            logging.info(f"DataFrame shape: {df_to_save.shape}")
            logging.info(f"DataFrame columns: {list(df_to_save.columns)}")
            raise
