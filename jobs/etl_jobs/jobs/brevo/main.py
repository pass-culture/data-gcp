from datetime import datetime, timedelta, timezone
import typer
import pandas as pd
from google.cloud import bigquery
import logging

from connectors.brevo import BrevoConnector
from http_custom.clients import SyncHttpClient
from jobs.brevo.utils import (
    GCP_PROJECT,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
    access_secret_data,
    campaigns_histo_schema,
    transactional_histo_schema,
    ENV_SHORT_NAME,
)

from jobs.brevo.utils import SyncBrevoHeaderRateLimiter

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer()

TRANSACTIONAL_TABLE_NAME = "sendinblue_transactional_detailed"
UPDATE_WINDOW = 7 if ENV_SHORT_NAME == "prod" else 14

def save_df_to_bq(df: pd.DataFrame, table_id: str, schema: dict, partition_field: str = "update_date"):
    logger.info(f"Saving dataframe to BigQuery: {table_id}")
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        ),
        schema=[
            bigquery.SchemaField(name, field_type) for name, field_type in schema.items()
        ],
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Upload to BigQuery completed.")


@app.command()
def run(
    target: str = typer.Option(..., help="Nom de la tache (newsletter ou transactional)"),
    audience: str = typer.Option(..., help="Nom de l'audience (native ou pro)"),
    start_date: str = typer.Option(None, help="Date de début d'import (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, help="Date de fin d'import (YYYY-MM-DD)"),
):
    today = datetime.now(tz=timezone.utc)
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else (today - timedelta(days=UPDATE_WINDOW))
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else today

    logger.info(f"Running Brevo ETL with target={target}, audience={audience}, start={start_dt}, end={end_dt}")

    if audience == "native":
        api_key = access_secret_data(GCP_PROJECT, f"sendinblue-api-key-{ENV_SHORT_NAME}")
        table_name = "sendinblue_newsletters"
    elif audience == "pro":
        api_key = access_secret_data(GCP_PROJECT, f"sendinblue-pro-api-key-{ENV_SHORT_NAME}")
        table_name = "sendinblue_pro_newsletters"
    else:
        typer.echo("Invalid audience. Must be one of native/pro.")
        raise typer.Exit(code=1)

    rate_limiter = SyncBrevoHeaderRateLimiter()
    client = SyncHttpClient(rate_limiter=rate_limiter)
    connector = BrevoConnector(api_key=api_key, client=client)

    if target == "newsletter":
        logger.info("Fetching email campaigns...")
        resp = connector.get_email_campaigns()
        if not resp:
            typer.echo("No campaigns fetched.")
            return

        campaigns = resp.json().get("campaigns", [])
        logger.info(f"Fetched {len(campaigns)} campaigns")

        df = pd.DataFrame([
            {
                "campaign_id": c.get("id"),
                "campaign_utm": c.get("tag"),
                "campaign_name": c.get("name"),
                "campaign_sent_date": c.get("sentDate"),
                "share_link": c.get("shareLink"),
                "audience_size": c.get("statistics", {}).get("globalStats", {}).get("delivered"),
                "unsubscriptions": c.get("statistics", {}).get("globalStats", {}).get("unsubscriptions"),
                "open_number": c.get("statistics", {}).get("globalStats", {}).get("uniqueViews"),
                "update_date": pd.to_datetime("today"),
            }
            for c in campaigns
        ])
        df["campaign_target"] = audience

        partition_date = end_dt.strftime("%Y%m%d")
        table_id = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}_histo${partition_date}"
        save_df_to_bq(df, table_id, campaigns_histo_schema)

    elif target == "transactional":
        logger.info("Fetching active SMTP templates...")
        templates_resp = connector.get_smtp_templates(active_only=True)
        templates = templates_resp.json().get("templates", []) if templates_resp else []

        if not templates:
            typer.echo("No active templates found.")
            raise typer.Exit(code=0)

        logger.info(f"Fetched {len(templates)} active templates")

        all_events = []
        for template in templates:
            template_id = template.get("id")
            tag = template.get("tag")
            for event_type in ["delivered", "opened", "unsubscribed"]:
                offset = 0
                while True:
                    logger.info(f"Fetching events: template={template_id}, event={event_type}, offset={offset}")
                    events_resp = connector.get_email_event_report(
                        template_id=template_id,
                        event=event_type,
                        start_date=start_dt.strftime("%Y-%m-%d"),
                        end_date=end_dt.strftime("%Y-%m-%d"),
                        offset=offset
                    )
                    if not events_resp:
                        logger.warning("No response received. Skipping.")
                        break

                    events = events_resp.json().get("events", [])
                    if not events:
                        logger.info("No more events in page.")
                        break

                    for event in events:
                        all_events.append({
                            "template": template_id,
                            "tag": tag,
                            "email": event.get("email"),
                            "event": event.get("event"),
                            "event_date": pd.to_datetime(event.get("date")).date(),
                        })

                    if len(events) < 2500:
                        break
                    offset += 2500

        if not all_events:
            typer.echo("No events collected.")
            return

        df = pd.DataFrame(all_events)
        logger.info(f"Collected {len(df)} events")

        df_kpis = df.groupby(["tag", "template", "email", "event", "event_date"]).agg({"event_date": "count"})
        df_kpis.columns = ["count"]
        df_kpis.reset_index(inplace=True)

        df_pivot = pd.pivot_table(
            df_kpis,
            values="count",
            index=["tag", "template", "email", "event_date"],
            columns=["event"],
            aggfunc="sum",
            fill_value=0,
        ).reset_index()

        for col in ["delivered", "opened", "unsubscribed"]:
            if col not in df_pivot:
                df_pivot[col] = 0

        df_pivot.rename(columns={
            "delivered": "delivered_count",
            "opened": "opened_count",
            "unsubscribed": "unsubscribed_count"
        }, inplace=True)

        df_pivot["target"] = audience

        yyyymmdd = end_dt.strftime("%Y%m%d")
        table_id = f"{GCP_PROJECT}.{BIGQUERY_TMP_DATASET}.{yyyymmdd}_{TRANSACTIONAL_TABLE_NAME}_histo"
        save_df_to_bq(df_pivot, table_id, transactional_histo_schema, partition_field="event_date")

    else:
        typer.echo("Invalid target. Must be one of transactional/newsletter.")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
