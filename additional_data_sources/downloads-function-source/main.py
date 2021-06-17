from google.cloud import bigquery

from apple_client import AppleClient
from google_client import GoogleClient
from utils import (
    KEY_ID,
    ISSUER_ID,
    PRIVATE_KEY,
    BIGQUERY_RAW_DATASET,
    get_last_month,
    get_current_month,
)


def run(request):
    bigquery_client = bigquery.Client()
    last_month = get_last_month()
    current_month = get_current_month()

    apple_client = AppleClient(
        key_id=KEY_ID, issuer_id=ISSUER_ID, private_key=PRIVATE_KEY
    )
    last_month_apple_downloads = apple_client.get_monthly_donloads(last_month)
    current_month_apple_downloads = apple_client.get_monthly_donloads(current_month)

    google_client = GoogleClient(report_bucket_name="pubsite_prod_8102412585126803216")
    last_month_google_downloads = google_client.get_monthly_donloads(last_month)
    current_month_google_downloads = google_client.get_monthly_donloads(current_month)

    bigquery_client.query(
        f"DELETE FROM {BIGQUERY_RAW_DATASET}.app_downloads_stats WHERE month IN ('{last_month}', '{current_month}')"
    )
    bigquery_client.query(
        f"""
        INSERT {BIGQUERY_RAW_DATASET}.app_downloads_stats (date, apple_downloads, google_downloads)
        VALUES ('{last_month}', {last_month_apple_downloads}, {last_month_google_downloads}),
        ('{current_month}', {current_month_apple_downloads}, {current_month_google_downloads}), 
        """
    )

    return "Success"
