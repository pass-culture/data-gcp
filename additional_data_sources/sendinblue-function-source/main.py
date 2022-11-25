from sendinblue import sendinblue_newsletters
from datetime import datetime, timezone, timedelta
from utils import GCP_PROJECT, BIGQUERY_RAW_DATASET, ENV_SHORT_NAME, access_secret_data

API_KEY = access_secret_data(
    GCP_PROJECT, f"sendinblue-api-key-{ENV_SHORT_NAME}", version_id=1
)

NEWSLETTERS_TABLE_NAME = "sendinblue_newsletters"
UPDATE_WINDOW = 31 if ENV_SHORT_NAME == 'prod' else 500

today = datetime.now(tz=timezone.utc)

def run(request):
    # Statistics for email campaigns Sendinblue (by domain)

    sendinblue_request = sendinblue_newsletters(
        gcp_project=GCP_PROJECT,
        raw_dataset=BIGQUERY_RAW_DATASET,
        api_key=API_KEY,
        destination_table_name=NEWSLETTERS_TABLE_NAME,
        start_date=today - timedelta(days=UPDATE_WINDOW),
        end_date=today
        )

    sendinblue_request.create_instance_email_campaigns_api()
    df = sendinblue_request.get_data_by_domain()
    sendinblue_request.save_to_historical(df)

    return "success"
    # Statistics for transactional email Sendinblue (later)
