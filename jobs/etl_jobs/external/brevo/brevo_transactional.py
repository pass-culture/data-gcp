import logging
from datetime import datetime

import brevo_python
import numpy as np
import pandas as pd
from brevo_python.rest import ApiException
from google.cloud import bigquery

from utils import (
    ENV_SHORT_NAME,
    rate_limiter,
)


class BrevoTransactional:
    def __init__(
        self,
        gcp_project,
        tmp_dataset,
        destination_table_name,
        api_key,
        start_date,
        end_date,
    ):
        self.gcp_project = gcp_project
        self.tmp_dataset = tmp_dataset
        self.destination_table_name = destination_table_name
        self.api_key = api_key
        self.start_date = start_date
        self.end_date = end_date

    def create_instance_transactional_email_api(self):
        configuration = brevo_python.Configuration()
        configuration.api_key["api-key"] = self.api_key  # get secret

        api_instance = brevo_python.TransactionalEmailsApi(
            brevo_python.ApiClient(configuration)
        )

        self.api_instance = api_instance

    def get_active_templates_id(self):
        active_templates = []
        try:
            offset = 0
            response = self.api_instance.get_smtp_templates(
                template_status="true", offset=offset
            )
            api_responses = response.templates
            if ENV_SHORT_NAME == "prod":
                while response is not None and len(response.templates) == 50:
                    offset = offset + 50
                    response = self.api_instance.get_smtp_templates(
                        template_status="true", offset=offset
                    )
                    for temp in response.templates:
                        api_responses.append(temp)

            active_templates = [template.id for template in api_responses]
            logging.info(f"Number of active templates: {len(active_templates)}")
        except ApiException as e:
            logging.error(
                f"Exception when calling TransactionalEmailsApi->get_smtp_templates: {e}"
            )

        return active_templates

    def check_rate_limit_status(self):
        """Check current rate limit status before starting."""
        try:
            # Make a lightweight API call to check status
            self.api_instance.get_smtp_templates(
                template_status="true", limit=1, offset=0
            )
            logging.info("Rate limit check: API is accessible")
            return True
        except ApiException as e:
            if e.status == 429:
                if hasattr(e, "http_resp") and hasattr(e.http_resp, "headers"):
                    headers = e.http_resp.headers
                    reset_time = headers.get("x-sib-ratelimit-reset")
                    remaining = headers.get("x-sib-ratelimit-remaining")
                    limit = headers.get("x-sib-ratelimit-limit")

                    logging.warning("Rate limit exceeded!")
                    logging.warning(f"  Limit: {limit}, Remaining: {remaining}")
                    if reset_time:
                        logging.warning(
                            f"  Reset in: {reset_time} seconds ({int(reset_time)/60:.1f} minutes)"
                        )
                    else:
                        logging.warning("  Reset time unknown")
                    return False
                else:
                    logging.warning("Rate limit exceeded (no details available)")
                    return False
            raise

    # Use the actual Brevo API limit with safety margin
    @rate_limiter(
        calls=280, period=3600, log_waits=True
    )  # 280 calls per hour (safe margin from 300)
    def _get_email_event_report(self, **kwargs):
        return self.api_instance.get_email_event_report(**kwargs)

    def get_events(self, event_type):
        # Check rate limit status before starting
        if not self.check_rate_limit_status():
            logging.error("Rate limit exceeded. Please wait before retrying.")
            return []

        active_templates = self.get_active_templates_id()
        if ENV_SHORT_NAME != "prod" and len(active_templates) > 0:
            active_templates = active_templates[:1]

        logging.info(f"Number of active templates: {len(active_templates)}")
        logging.info(f"Processing event type: {event_type}")

        api_responses = []
        total_calls = 0

        for i, template in enumerate(active_templates):
            logging.info(
                f"Processing template {i+1}/{len(active_templates)}: {template}"
            )
            offset = 0
            template_events = []

            try:
                response = self._get_email_event_report(
                    template_id=template,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    event=event_type,
                    offset=offset,
                )
                total_calls += 1

                if response and response.events:
                    template_events.extend(response.events)
                    logging.info(f"  Initial batch: {len(response.events)} events")

                    # Continue pagination for production
                    if ENV_SHORT_NAME == "prod":
                        # Continue pagination while we get full pages
                        while (
                            response
                            and response.events
                            and len(response.events) == 2500
                        ):
                            offset += 2500
                            logging.info(
                                f"  Fetching offset {offset} for template {template}"
                            )

                            response = self._get_email_event_report(
                                template_id=template,
                                start_date=self.start_date,
                                end_date=self.end_date,
                                event=event_type,
                                offset=offset,
                            )
                            total_calls += 1

                            if response and response.events:
                                template_events.extend(response.events)
                                logging.info(
                                    f"  Batch at offset {offset}: {len(response.events)} events"
                                )
                else:
                    logging.info(f"  No events found for template {template}")

            except ApiException as e:
                if e.status == 429:
                    logging.error(
                        f"Rate limit exceeded while processing template {template}"
                    )
                    logging.error(
                        "Stopping processing to avoid further rate limit issues"
                    )
                    break
                else:
                    logging.error(f"  Error processing template {template}: {e}")
                    continue
            except Exception as e:
                logging.error(f"  Unexpected error processing template {template}: {e}")
                continue

            if template_events:
                api_responses.extend(template_events)

            logging.info(f"  Template {template} total events: {len(template_events)}")
            logging.info(f"  Total API calls so far: {total_calls}")

            # Progress indicator
            if len(active_templates) > 1:
                progress = (i + 1) / len(active_templates) * 100
                logging.info(
                    f"  Progress: {progress:.1f}% ({i+1}/{len(active_templates)} templates)"
                )

        logging.info(f"Total events collected for {event_type}: {len(api_responses)}")
        logging.info(f"Total API calls made: {total_calls}")

        return api_responses

    def parse_to_df(self, all_events):
        if not all_events:
            logging.warning("No events to parse")
            # Return empty dataframe with expected columns
            columns = [
                "template",
                "tag",
                "email",
                "event_date",
                "delivered_count",
                "opened_count",
                "unsubscribed_count",
            ]
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame()
        df = (
            df.assign(email=[event.email for event in all_events])
            .assign(event=[event.event for event in all_events])
            .assign(template=[event.template_id for event in all_events])
            .assign(event_date=pd.to_datetime([event._date for event in all_events]))
            .assign(tag=[event.tag for event in all_events])
        )

        df["event_date"] = df["event_date"].dt.date

        df_grouped = df.groupby(
            ["tag", "template", "email", "event", "event_date"]
        ).agg({"event_date": ["count"]})

        df_grouped.columns = df_grouped.columns.map("_".join)
        df_grouped.reset_index(inplace=True)

        df_kpis = pd.pivot_table(
            df_grouped,
            values=["event_date_count"],
            index=["tag", "template", "email", "event_date"],
            columns=["event"],
            aggfunc={"event_date_count": "sum"},
        )

        df_kpis.columns = df_kpis.columns.map("_".join)
        df_kpis.reset_index(inplace=True)
        df_kpis.fillna(0, inplace=True)
        df_kpis.rename(
            columns={
                "event_date_count_delivered": "delivered_count",
                "event_date_count_opened": "opened_count",
                "event_date_count_unsubscribed": "unsubscribed_count",
            },
            inplace=True,
        )

        columns = [
            "template",
            "tag",
            "email",
            "event_date",
            "delivered_count",
            "opened_count",
            "unsubscribed_count",
        ]

        for column in columns:
            if column not in df_kpis:
                df_kpis[column] = np.nan

        df_kpis = df_kpis[columns]

        return df_kpis

    def save_to_historical(self, df_to_save, schema):
        if df_to_save.empty:
            logging.warning("No data to save")
            return

        bigquery_client = bigquery.Client()

        yyyymmdd = datetime.today().strftime("%Y%m%d")
        table_id = f"{self.gcp_project}.{self.tmp_dataset}.{yyyymmdd}_{self.destination_table_name}_histo"

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField(column, _type)
                    for column, _type in schema.items()
                ],
            )

            logging.info(f"Saving {len(df_to_save)} rows to BigQuery...")
            job = bigquery_client.load_table_from_dataframe(
                df_to_save, table_id, job_config=job_config
            )
            job.result()
            logging.info(f"Successfully saved {len(df_to_save)} rows to {table_id}")

        except Exception as e:
            logging.error(f"Error saving to BigQuery: {e}")
            logging.info(f"Table ID: {table_id}")
            logging.info(f"DataFrame shape: {df_to_save.shape}")
            logging.info(f"DataFrame columns: {list(df_to_save.columns)}")
            raise
