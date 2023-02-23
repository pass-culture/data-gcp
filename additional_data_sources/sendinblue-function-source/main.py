from sendinblue_newsletters import SendinblueNewsletters
from sendinblue_transactional import SendinblueTransactional
from datetime import datetime, timezone, timedelta, date
from utils import (
    GCP_PROJECT,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    access_secret_data,
    campaigns_histo_schema,
    # transactional_histo_schema,
)


API_KEY = access_secret_data(
    GCP_PROJECT, f"sendinblue-api-key-{ENV_SHORT_NAME}", version_id=1
)

NEWSLETTERS_TABLE_NAME = "sendinblue_newsletters"
TRANSACTIONAL_TABLE_NAME = "sendinblue_transactional"
UPDATE_WINDOW = 31 if ENV_SHORT_NAME == "prod" else 500

today = datetime.now(tz=timezone.utc)
yesterday = date.today() - timedelta(days=1)


def run(request):

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and "target" in request_json:
        target = request_json["target"]
    elif request_args and "target" in request_args:
        target = request_args["target"]
    else:
        raise RuntimeError("You need to provide a target argument.")

    if target == "newsletter":
        # Statistics for email campaigns Sendinblue
        sendinblue_newsletters = SendinblueNewsletters(
            gcp_project=GCP_PROJECT,
            raw_dataset=BIGQUERY_RAW_DATASET,
            api_key=API_KEY,
            destination_table_name=NEWSLETTERS_TABLE_NAME,
            start_date=today - timedelta(days=UPDATE_WINDOW),
            end_date=today,
        )

        sendinblue_newsletters.create_instance_email_campaigns_api()
        df = sendinblue_newsletters.get_data()
        sendinblue_newsletters.save_to_historical(df, campaigns_histo_schema)
        return "success"

    elif target == "transactional":
        # Statistics for transactional email Sendinblue
        sendinblue_transactional = SendinblueTransactional(
            gcp_project=GCP_PROJECT,
            raw_dataset=BIGQUERY_RAW_DATASET,
            api_key=API_KEY,
            destination_table_name=TRANSACTIONAL_TABLE_NAME,
            start_date=yesterday.strftime("%Y-%m-%d"),
            end_date=yesterday.strftime("%Y-%m-%d"),
        )
        sendinblue_transactional.create_instance_transactional_email_api()
        all_events = []
        for event_type in ["delivered", "opened", "unsubscribed"]:
            all_events.append(sendinblue_transactional.get_events(event_type))
        all_events = sum(all_events, [])
        df = sendinblue_transactional.parse_to_df(all_events)
        sendinblue_transactional.save_to_historical(df)

        return "success"

    else:
        return "Invalid target. Must be one of transactional/newsletter."
