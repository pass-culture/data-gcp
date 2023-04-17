import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

from google.cloud import bigquery
import pandas as pd
import numpy as np

import logging


class SendinblueTransactional:
    def __init__(
        self,
        gcp_project,
        raw_dataset,
        destination_table_name,
        api_key,
        start_date,
        end_date,
    ):
        self.gcp_project = gcp_project
        self.raw_dataset = raw_dataset
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

        print(f"Number of active templates : {len(active_templates)}")

        api_responses = []
        for template in active_templates:
            offset = 0
            response = self.api_instance.get_email_event_report(
                template_id=template,
                start_date=self.start_date,
                end_date=self.end_date,
                event=event_type,
                offset=offset,
            ).events
            if response is not None:
                logging.info(
                    f"Number of responses for template {template}: {len(response)}"
                )
                api_responses.append(response)
                while response is not None and len(response) == 2500:
                    offset = offset + 2500
                    logging.info(f"Importing offset {offset} for template {template} ")
                    response = self.api_instance.get_email_event_report(
                        template_id=template,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        event=event_type,
                        offset=offset,
                    ).events
                    api_responses.append(response)

        api_responses = sum(
            api_responses, []
        )  # concatener tous les events dans une unique liste

        logging.info(
            f"Number of active templates with events between {self.start_date} and {self.end_date} : {len(api_responses)}"
        )

        return api_responses

    def parse_to_df(self, all_events):

        df = pd.DataFrame()

        df = (
            df.assign(email=[event.email for event in all_events])
            .assign(event=[event.event for event in all_events])
            .assign(template=[event.template_id for event in all_events])
            .assign(event_date=[event._date for event in all_events])
            .assign(tag=[event.tag for event in all_events])
            .drop_duplicates()
            .groupby(["template", "event", "tag"], dropna=False)
            .agg(
                unique=("email", "nunique"),
                count=("email", "count"),
                first_date=("event_date", "min"),
                last_date=("event_date", "max"),
            )
            .reset_index()
            .pivot_table(
                index=["template", "tag"],
                values=["unique", "count", "first_date", "last_date"],
                columns="event",
                aggfunc={
                    "first_date": "min",
                    "last_date": "max",
                    "unique": "sum",
                    "count": "sum",
                },
            )
        )

        df.columns = df.columns.map("_".join)

        df = df.reset_index().assign(update_date=pd.to_datetime(self.start_date))

        columns = [
            "template",
            "tag",
            "count_delivered",
            "first_date_delivered",
            "last_date_delivered",
            "unique_delivered",
            "count_opened",
            "first_date_opened",
            "last_date_opened",
            "unique_opened",
            "count_unsubscribed",
            "first_date_unsubscribed",
            "last_date_unsubscribed",
            "unique_unsubscribed",
            "update_date",
        ]

        for column in columns:
            if column not in df:
                df[column] = np.nan

        df = df[columns]

        # convert to datetime
        df = df.assign(
            first_date_delivered=lambda _df: pd.to_datetime(
                _df["first_date_delivered"]
            ),
            last_date_delivered=lambda _df: pd.to_datetime(_df["last_date_delivered"]),
            first_date_opened=lambda _df: pd.to_datetime(_df["first_date_opened"]),
            last_date_opened=lambda _df: pd.to_datetime(_df["last_date_opened"]),
            first_date_unsubscribed=lambda _df: pd.to_datetime(
                _df["first_date_unsubscribed"]
            ),
            last_date_unsubscribed=lambda _df: pd.to_datetime(
                _df["last_date_unsubscribed"]
            ),
        )

        return df

    def save_to_historical(self, df_to_save, schema):

        bigquery_client = bigquery.Client()

        yyyymmdd = self.start_date.replace("-", "")
        table_id = f"{self.gcp_project}.{self.raw_dataset}.{self.destination_table_name}_histo${yyyymmdd}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="update_date"
            ),
            schema=[
                bigquery.SchemaField(column, _type) for column, _type in schema.items()
            ],
        )
        job = bigquery_client.load_table_from_dataframe(
            df_to_save, table_id, job_config=job_config
        )
        job.result()
