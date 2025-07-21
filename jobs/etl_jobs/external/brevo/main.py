from datetime import date, datetime, timedelta, timezone

import typer

from brevo_newsletters import BrevoNewsletters
from brevo_transactional import BrevoTransactional
from utils import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    access_secret_data,
    campaigns_histo_schema,
    transactional_histo_schema,
)

NATIVE_API_KEY = access_secret_data(GCP_PROJECT, f"sendinblue-api-key-{ENV_SHORT_NAME}")
NATIVE_API_KEY_2 = access_secret_data(
    GCP_PROJECT, f"sendinblue-api-key-2-{ENV_SHORT_NAME}"
)

PRO_API_KEY = access_secret_data(
    GCP_PROJECT, f"sendinblue-pro-api-key-{ENV_SHORT_NAME}"
)
PRO_API_KEY_2 = access_secret_data(
    GCP_PROJECT, f"sendinblue-pro-api-key-2-{ENV_SHORT_NAME}"
)


TRANSACTIONAL_TABLE_NAME = "sendinblue_transactional_detailed"
UPDATE_WINDOW = 31 if ENV_SHORT_NAME == "prod" else 500

today = datetime.now(tz=timezone.utc)
yesterday = date.today() - timedelta(days=1)


def run(
    target: str = typer.Option(
        ...,
        help="Nom de la tache",
    ),
    audience: str = typer.Option(
        ...,
        help="Nom de l'audience, native ou pro",
    ),
    start_date: str = typer.Option(..., help="Date de début d'import"),
    end_date: str = typer.Option(..., help="Date de fin d'import"),
):
    if audience == "native":
        API_KEY = NATIVE_API_KEY
        API_KEY_2 = NATIVE_API_KEY_2
        NEWSLETTERS_TABLE_NAME = "sendinblue_newsletters"
    elif audience == "pro":
        API_KEY = PRO_API_KEY
        API_KEY_2 = PRO_API_KEY_2
        NEWSLETTERS_TABLE_NAME = "sendinblue_pro_newsletters"
    else:
        return "Invalid audience. Must be one of native/pro."

    if target == "newsletter":
        # Statistics for email campaigns Brevo
        brevo_newsletters = BrevoNewsletters(
            gcp_project=GCP_PROJECT,
            raw_dataset=BIGQUERY_RAW_DATASET,
            api_key=API_KEY,
            destination_table_name=NEWSLETTERS_TABLE_NAME,
            start_date=today - timedelta(days=UPDATE_WINDOW),
            end_date=today,
        )

        brevo_newsletters.create_instance_email_campaigns_api()
        df = brevo_newsletters.get_data()
        df["campaign_target"] = audience
        brevo_newsletters.save_to_historical(df, campaigns_histo_schema)
        return "success"

    elif target == "transactional":
        # Statistics for transactional email Brevo
        brevo_transactional = BrevoTransactional(
            gcp_project=GCP_PROJECT,
            tmp_dataset=BIGQUERY_TMP_DATASET,
            api_keys=[API_KEY, API_KEY_2],
            destination_table_name=TRANSACTIONAL_TABLE_NAME,
            start_date=start_date,
            end_date=end_date,
        )
        brevo_transactional.create_instance_transactional_email_api()
        all_events = []
        for event_type in ["delivered", "opened", "unsubscribed"]:
            all_events.append(brevo_transactional.get_events(event_type))
        all_events = sum(all_events, [])
        df = brevo_transactional.parse_to_df(all_events)
        df["target"] = audience
        brevo_transactional.save_to_historical(df, transactional_histo_schema)

        return "success"

    else:
        return "Invalid target. Must be one of transactional/newsletter."


if __name__ == "__main__":
    typer.run(run)
