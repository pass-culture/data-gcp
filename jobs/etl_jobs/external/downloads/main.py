from google.cloud import bigquery
from time import sleep
from datetime import datetime, timedelta
from apple_client import AppleClient
from google_client import GoogleClient
import pandas as pd
from utils import (
    KEY_ID,
    ISSUER_ID,
    PRIVATE_KEY,
    BIGQUERY_RAW_DATASET,
    BUCKET_NAME,
    get_last_month,
)


def get_apple(execution_date):

    start = get_last_month(execution_date)
    date_generated = [
        start + timedelta(days=x) for x in range(0, (execution_date - start).days)
    ]
    date_generated_str = [x.strftime("%Y-%m-%d") for x in date_generated]
    date_generated_str_join = "','".join(date_generated_str)
    apple_client = AppleClient(
        key_id=KEY_ID, issuer_id=ISSUER_ID, private_key=PRIVATE_KEY
    )
    dfs = []
    for d in date_generated_str:
        _df = apple_client.get_downloads(frequency="DAILY", report_date=d)
        if _df is not None:
            dfs.append(_df)
        sleep(0.5)
    df = pd.concat(dfs)
    bigquery_client = bigquery.Client()
    try:
        delete_query = bigquery_client.query(
            f"DELETE FROM {BIGQUERY_RAW_DATASET}.apple_download_stats WHERE date IN ('{date_generated_str_join}')"
        )
        delete_query.result()
    except:
        pass
    df.to_gbq(f"{BIGQUERY_RAW_DATASET}.apple_download_stats", if_exists="append")


def get_google(execution_date):
    current_month = execution_date.strftime("%Y-%m")
    last_month = get_last_month(execution_date).strftime("%Y-%m")
    google_client = GoogleClient(report_bucket_name=BUCKET_NAME)
    df = google_client.get_downloads(last_month)
    try:
        current_month_google_downloads = google_client.get_downloads(current_month)
        df = pd.concat([df, current_month_google_downloads])
    except:
        pass

    date_generated_str_join = "','".join(list(df["date"].unique()))
    try:
        bigquery_client = bigquery.Client()
        delete_query = bigquery_client.query(
            f"DELETE FROM {BIGQUERY_RAW_DATASET}.google_download_stats WHERE date IN ('{date_generated_str_join}')"
        )
        delete_query.result()
    except:
        pass
    df.to_gbq(f"{BIGQUERY_RAW_DATASET}.google_download_stats", if_exists="append")


def run():

    execution_date = datetime.today()

    get_google(execution_date)
    get_apple(execution_date)

    return "Success"


run()
