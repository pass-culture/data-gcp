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
    get_current_day,
)


def run(request):
    bigquery_client = bigquery.Client()
    last_month = get_last_month()
    current_month = get_current_month()
    current_day = get_current_day()

    apple_client = AppleClient(
        key_id=KEY_ID, issuer_id=ISSUER_ID, private_key=PRIVATE_KEY
    )
    last_month_apple_downloads = apple_client.get_downloads(
        frequency="MONTHLY", report_date=last_month
    )
    current_month_apple_downloads = 0
    for day in range(1, int(current_day)):
        day_string = f"0{day}" if day < 10 else str(day)
        current_month_apple_downloads += apple_client.get_downloads(
            frequency="DAILY", report_date=f"{current_month}-{day_string}"
        )

    google_client = GoogleClient(report_bucket_name="pubsite_prod_8102412585126803216")
    last_month_google_downloads = google_client.get_monthly_downloads(last_month)
    try:
        current_month_google_downloads = google_client.get_monthly_downloads(
            current_month
        )
    except:
        current_month_google_downloads = None

    delete_query = bigquery_client.query(
        f"DELETE FROM {BIGQUERY_RAW_DATASET}.app_downloads_stats WHERE month IN ('{last_month}', '{current_month}')"
    )
    delete_query.result()

    for values in [
        {
            "month": last_month,
            "apple": int(last_month_apple_downloads),
            "google": int(last_month_google_downloads),
        },
        {
            "month": current_month,
            "apple": int(current_month_apple_downloads)
            if current_month_apple_downloads
            else "null",
            "google": int(current_month_google_downloads)
            if current_month_google_downloads
            else "null",
        },
    ]:
        write_query = bigquery_client.query(
            f"""
            INSERT {BIGQUERY_RAW_DATASET}.app_downloads_stats (month, apple_downloads, google_downloads)
            VALUES ('{values["month"]}', {values["apple"]}, {values["google"]})
            """
        )
        write_query.result()

    return "Success"
