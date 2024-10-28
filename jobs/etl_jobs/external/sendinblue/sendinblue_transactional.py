import logging
import time
from datetime import datetime

import numpy as np
import pandas as pd
import sib_api_v3_sdk
from google.cloud import bigquery
from sib_api_v3_sdk.rest import ApiException

from utils import ENV_SHORT_NAME


class SendinblueTransactional:
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
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key["api-key"] = self.api_key  # get secret

        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )

        self.api_instance = api_instance

    def get_active_templates_id(self):
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
            logging.info("Number of active templates : ", len(active_templates))
        except ApiException as e:
            logging.info(
                "Exception when calling TransactionalEmailsApi->get_smtp_templates: %s\n"
                % e
            )

        return active_templates


def get_events(self, event_type, max_retries=5, initial_delay=5):
    active_templates = self.get_active_templates_id()
    if ENV_SHORT_NAME != "prod" and len(active_templates) > 0:
        active_templates = active_templates[:1]  # limit to 1 for non-prod environments

    print(f"Number of active templates: {len(active_templates)}")

    api_responses = []
    for template in active_templates:
        offset = 0
        while True:
            for attempt in range(max_retries):
                try:
                    # Fetch events with the current offset
                    response = self.api_instance.get_email_event_report(
                        template_id=template,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        event=event_type,
                        offset=offset,
                    ).events

                    if response:
                        logging.info(
                            f"Fetched {len(response)} events for template {template} at offset {offset}"
                        )
                        api_responses.extend(response)

                        # Break the retry loop on successful fetch
                        break

                    # Exit if no more data is available
                    if not response or len(response) < 2500:
                        break

                except ApiException as e:
                    if e.status == 429:  # Too many requests
                        delay = initial_delay * (2**attempt)  # exponential backoff
                        logging.warning(
                            f"Rate limit hit. Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                    else:
                        logging.error(
                            f"Failed to fetch events for template {template}: {e}"
                        )
                        break  # break retry loop on non-retryable error

            # Update offset for pagination; exit loop if fewer than max results are returned
            offset += 2500
            if not response or len(response) < 2500:
                break  # exit loop if last response was less than max batch size

    logging.info(
        f"Total events fetched for templates between {self.start_date} and {self.end_date}: {len(api_responses)}"
    )

    return api_responses

    def parse_to_df(self, all_events):
        df = pd.DataFrame()
        df = (
            df.assign(email=[event.email for event in all_events])
            .assign(event=[event.event for event in all_events])
            .assign(template=[event.template_id for event in all_events])
            .assign(event_date=pd.to_datetime([event._date for event in all_events]))
            .assign(tag=[event.tag for event in all_events])
            .drop_duplicates()
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
        bigquery_client = bigquery.Client()

        yyyymmdd = datetime.today().strftime("%Y%m%d")
        table_id = f"{self.gcp_project}.{self.tmp_dataset}.{yyyymmdd}_{self.destination_table_name}_histo"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField(column, _type) for column, _type in schema.items()
            ],
        )
        job = bigquery_client.load_table_from_dataframe(
            df_to_save, table_id, job_config=job_config
        )
        job.result()
