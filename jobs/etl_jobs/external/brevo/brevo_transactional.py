import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List

import brevo_python
import numpy as np
import pandas as pd
from brevo_python.rest import ApiException
from google.cloud import bigquery

from utils import (
    ENV_SHORT_NAME,
    rate_limiter,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class BrevoTransactional:
    def __init__(
        self,
        gcp_project,
        tmp_dataset,
        destination_table_name,
        api_keys: List[str],
        start_date,
        end_date,
    ):
        self.gcp_project = gcp_project
        self.tmp_dataset = tmp_dataset
        self.destination_table_name = destination_table_name
        self.api_keys = (
            api_keys if isinstance(api_keys, list) else [api_keys]
        )  # Ensure it's a list
        self.start_date = start_date
        self.end_date = end_date
        self.api_instances = []

    def create_instance_transactional_email_api(self):
        # Create an API instance for each API key
        for i, api_key in enumerate(self.api_keys):
            configuration = brevo_python.Configuration()
            configuration.api_key["api-key"] = api_key
            api_instance = brevo_python.TransactionalEmailsApi(
                brevo_python.ApiClient(configuration)
            )
            # Store the instance with its index for identification
            api_instance.instance_id = i
            self.api_instances.append(api_instance)

        # For backward compatibility, set the first instance as the default
        if self.api_instances:
            self.api_instance = self.api_instances[0]

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
            logger.info("Number of active templates : %s", len(active_templates))
        except ApiException as e:
            logger.info(
                "Exception when calling TransactionalEmailsApi->get_smtp_templates: %s\n"
                % e
            )

        return active_templates

    # https://developers.brevo.com/docs/api-limits
    @rate_limiter(calls=280, period=3600)  # Custom rate limiter: 280 calls per hour
    def _get_email_event_report(self, api_instance, **kwargs):
        max_retries = 3
        retry_count = 0

        while retry_count <= max_retries:
            try:
                return api_instance.get_email_event_report(**kwargs)
            except ApiException as e:
                transient_error_statuses = [500, 502, 503, 504]
                # Check if it's a rate limit error (429)
                if e.status == 429:
                    # Extract the reset time from headers
                    reset_seconds = 600  # Default fallback
                    try:
                        headers = e.headers
                        if headers and "x-sib-ratelimit-reset" in headers:
                            reset_seconds = int(headers["x-sib-ratelimit-reset"])
                            # Add 10% buffer as requested
                            reset_seconds = reset_seconds * 1.1
                    except (ValueError, KeyError, AttributeError) as header_error:
                        logger.warning(
                            f"Could not extract reset time from headers: {header_error}"
                        )

                    instance_id = getattr(api_instance, "instance_id", "unknown")
                    logger.warning(
                        f"[API Instance {instance_id}] Rate limit exceeded. Waiting for {reset_seconds:.1f} seconds before retry."
                    )
                    time.sleep(reset_seconds)
                    retry_count += 1
                elif e.status in transient_error_statuses:
                    # For other transient errors, wait and retry
                    instance_id = getattr(api_instance, "instance_id", "unknown")
                    logger.warning(
                        f"[API Instance {instance_id}] Transient error {e.status}. Retrying in 10 seconds..."
                    )
                    time.sleep(10)
                    retry_count += 1
                else:
                    # For other API exceptions, just raise them
                    raise

        # If we've exhausted all retries
        raise Exception(
            f"Maximum retries ({max_retries}) exceeded for API call due to rate limiting"
        )

    def _process_template(self, template, event_type, api_instance):
        """Process a single template using the specified API instance"""
        instance_id = getattr(api_instance, "instance_id", "unknown")

        template_responses = []
        offset = 0
        response = self._get_email_event_report(
            api_instance=api_instance,
            template_id=template,
            start_date=self.start_date,
            end_date=self.end_date,
            event=event_type,
            offset=offset,
        ).events

        if response is None:
            logger.info(
                f"[API Instance {instance_id}] No responses for template {template}"
            )
            return template_responses

        logger.info(
            f"[API Instance {instance_id}] Number of responses for template {template}: {len(response)}"
        )
        template_responses.append(response)

        if ENV_SHORT_NAME != "prod":
            return template_responses

        while response is not None and len(response) == 2500:
            offset = offset + 2500
            logger.info(
                f"[API Instance {instance_id}] Importing offset {offset} for template {template}"
            )
            response = self._get_email_event_report(
                api_instance=api_instance,
                template_id=template,
                start_date=self.start_date,
                end_date=self.end_date,
                event=event_type,
                offset=offset,
            ).events
            if response is not None:
                template_responses.append(response)

        return template_responses

    def get_events(self, event_type):
        active_templates = self.get_active_templates_id()
        if ENV_SHORT_NAME != "prod" and len(active_templates) > 0:
            active_templates = active_templates[:1]

        logger.info(f"Number of active templates : {len(active_templates)}")

        # If no API instances are available, create them
        if not self.api_instances:
            self.create_instance_transactional_email_api()

        # Distribute templates among available API keys
        template_distribution = []
        num_api_instances = len(self.api_instances)

        for i, template in enumerate(active_templates):
            # Assign each template to an API instance in a round-robin fashion
            api_instance_index = i % num_api_instances
            template_distribution.append(
                (template, self.api_instances[api_instance_index])
            )

        api_responses = []
        # Process templates in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=num_api_instances) as executor:
            # Submit tasks for each template with its assigned API instance
            futures = [
                executor.submit(
                    self._process_template, template, event_type, api_instance
                )
                for template, api_instance in template_distribution
            ]

            # Collect results as they complete
            for future in futures:
                template_responses = future.result()
                if template_responses:
                    api_responses.extend(template_responses)

        # Flatten the list of responses
        api_responses = sum(
            api_responses, []
        )  # concatenate all events into a single list

        logger.info(
            f"Number of events between {self.start_date} and {self.end_date} for event type {event_type}: {len(api_responses)}"
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
