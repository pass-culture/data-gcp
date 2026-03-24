from datetime import datetime, timedelta
from enum import Enum
from time import sleep
from typing import Optional

import pandas as pd
import typer
from google.cloud import bigquery
from loguru import logger
from pandas_gbq import to_gbq

from apple_client import AppleClient
from google_client import GoogleClient
from utils import (
    BIGQUERY_RAW_DATASET,
    BUCKET_NAME,
    ISSUER_ID,
    KEY_ID,
    PRIVATE_KEY,
    get_last_month,
)

app = typer.Typer()


class Target(str, Enum):
    apple = "apple"
    google = "google"


def get_apple(execution_date: datetime):
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
    except Exception:
        pass

    to_gbq(df, f"{BIGQUERY_RAW_DATASET}.apple_download_stats", if_exists="append")


def get_google(execution_date: datetime):
    current_month = execution_date.strftime("%Y-%m")
    last_month = get_last_month(execution_date).strftime("%Y-%m")
    google_client = GoogleClient(report_bucket_name=BUCKET_NAME)
    df = google_client.get_downloads(last_month)
    try:
        current_month_google_downloads = google_client.get_downloads(current_month)
        df = pd.concat([df, current_month_google_downloads])
    except Exception:
        pass

    date_generated_str_join = "','".join(list(df["date"].unique()))
    try:
        bigquery_client = bigquery.Client()
        delete_query = bigquery_client.query(
            f"DELETE FROM {BIGQUERY_RAW_DATASET}.google_download_stats WHERE date IN ('{date_generated_str_join}')"
        )
        delete_query.result()
    except Exception:
        pass
    to_gbq(df, f"{BIGQUERY_RAW_DATASET}.google_download_stats", if_exists="append")


@app.command()
def run(
    target: Target = typer.Option(
        ..., help="Store to fetch downloads from appstore provider."
    ),
    execution_date: Optional[str] = typer.Option(
        None,
        help="Execution date (YYYY-MM-DD). Defaults to today.",
    ),
):
    date = (
        datetime.strptime(execution_date, "%Y-%m-%d")
        if execution_date
        else datetime.today()
    )
    logger.info(f"Running downloads job | target={target.value} | date={date.date()}")

    if target == Target.google:
        get_google(date)
    elif target == Target.apple:
        get_apple(date)

    logger.info("Done")


if __name__ == "__main__":
    app()
