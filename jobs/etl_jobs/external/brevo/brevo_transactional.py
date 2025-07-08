import logging
import time
from datetime import datetime

import brevo_python
import numpy as np
import pandas as pd
from brevo_python.rest import ApiException
from google.cloud import bigquery

from utils import ENV_SHORT_NAME


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
            logging.info("Number of active templates : ", len(active_templates))
        except ApiException as e:
            logging.info(
                "Exception when calling TransactionalEmailsApi->get_smtp_templates: %s\n"
                % e
            )

        return active_templates

    def get_events(self, event_type):
        active_templates = self.get_active_templates_id()
        if ENV_SHORT_NAME != "prod" and active_templates:
            active_templates = active_templates[:1]

        api_responses = []
        for template in active_templates:
            offset = 0
            while True:
                try:
                    resp = self.api_instance.get_email_event_report(
                        template_id=template,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        event=event_type,
                        offset=offset,
                    )
                    events = resp.events or []
                except ApiException as e:
                    # on 429, back off until reset
                    if e.status == 429:
                        reset_secs = int(e.headers.get("x-sib-ratelimit-reset", 60))
                        wait = reset_secs + 5
                        logging.warning(
                            "[get_events] rate limited on template %s for event %s; "
                            "reset in %ds, sleeping %ds then retrying",
                            template,
                            event_type,
                            reset_secs,
                            wait,
                        )
                        time.sleep(wait)
                        continue
                    else:
                        raise
                # collect and break if no more pages
                api_responses.append(events)
                if ENV_SHORT_NAME != "prod" or len(events) < 2500:
                    break
                offset += 2500
        # flatten
        all_events = sum(api_responses, [])
        print(f"Fetched {len(all_events)} events for {event_type}")
        return all_events

    def parse_to_df(self, all_events):
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
