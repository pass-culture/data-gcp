from sendinblue import sendinblue_email_campaigns

from utils import GCP_PROJECT, BIGQUERY_RAW_DATASET, ENV_SHORT_NAME, access_secret_data

API_KEY = access_secret_data(
    GCP_PROJECT, f"sendinblue-api-key-{ENV_SHORT_NAME}", version_id=1
)

EMAIL_CAMPAIGNS_TABLE_NAME = "sendinblue_email_campaigns"


def run(request):
    # Statistics for email campaigns Sendinblue (by domain)

    sendinblue_request = sendinblue_email_campaigns(
        gcp_project=GCP_PROJECT,
        raw_dataset=BIGQUERY_RAW_DATASET,
        api_key=API_KEY,
        destination_table_name=EMAIL_CAMPAIGNS_TABLE_NAME,
    )

    sendinblue_request.create_instance_email_campaigns_api()
    df = sendinblue_request.get_data_by_domain()
    sendinblue_request.save_to_raw(df)
    sendinblue_request.save_to_historical(df)

    return "success"
    # Statistics for transactional email Sendinblue (later)
