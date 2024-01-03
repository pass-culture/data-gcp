import os
import io
from google.cloud import bigquery
import requests
import polars as pl
import concurrent
import numpy as np
from multiprocessing import cpu_count
from itertools import repeat
from access_gcp_secrets import access_secret

GCP_PROJECT = os.environ.get("GCP_PROJECT", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
API_TOKEN_SECRET_ID = os.environ.get("API_TOKEN_SECRET_ID")
try:
    API_TOKEN = access_secret(GCP_PROJECT, API_TOKEN_SECRET_ID)
except:
    API_TOKEN = "test_token"
APP_CONFIG = {
    "URL": {
        "dev": "https://apireco.testing.passculture.team",
        "stg": "https://apireco.staging.passculture.team/",
        "prod": "https://apireco.passculture.app",
    },
    "TOKEN": API_TOKEN,
    "route": "similar_offers",
}
N_RECO_DISPLAY = 10


def get_offline_recos(data):
    max_process = 2 if ENV_SHORT_NAME == "dev" else cpu_count() - 2
    subset_length = len(data) // max_process
    subset_length = subset_length if subset_length > 0 else 1
    batch_number = max_process if subset_length > 1 else 1
    print(
        f"Starting process... with {batch_number} CPUs, subset length: {subset_length} "
    )
    batch_rows = [
        list(chunk)
        for chunk in list(np.array_split(data.rows(named=True), batch_number))
    ]

    with concurrent.futures.ProcessPoolExecutor(batch_number) as executor:
        futures = executor.map(
            _get_recos,
            batch_rows,
        )
    print("Multiprocessing done")
    dl_output = clean_multiprocess_output(futures)
    return dl_output


def _get_recos(rows):
    import requests

    results = []
    try:
        for row in rows:
            try:
                reco = similar_offers(
                    row["offer_id"], row["venue_longitude"], row["venue_latitude"]
                )[:N_RECO_DISPLAY]
            except:
                reco = []
            results.append(
                {"user_id": row["user_id"], "offer_id": row["offer_id"], "recos": reco}
            )
        return results
    except:
        return results


def similar_offers(offer_id, longitude, latitude):
    params_filter = {
        "is_reco_shuffled": False,
    }
    return call_API(offer_id, longitude, latitude, params_filter)["results"]


def call_API(input, longitude, latitude, params_filter):
    call = call_builder(input, longitude, latitude)
    return requests.post(call, json=params_filter).json()


def call_builder(input, longitude, latitude):
    call = f"{APP_CONFIG['URL'][ENV_SHORT_NAME]}/{APP_CONFIG['route']}/{input}?token={APP_CONFIG['TOKEN']}"
    if longitude is not None and latitude is not None:
        call = call + f"&longitude={longitude}&latitude={latitude}"
    return call


def clean_multiprocess_output(futures):
    user_ids = []
    recos = []
    for future in futures:
        for res in future:
            user_ids.append(res["user_id"])
            recos.append(res["recos"])
    dl_output = (
        pl.DataFrame({"user_id": user_ids, "recommendations": recos})
        .groupby("user_id")
        .agg(pl.concat_list("recommendations").flatten().unique().drop_nulls())
    )
    return dl_output


def export_polars_to_bq(client, data, dataset, output_table):
    with io.BytesIO() as stream:
        data.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=f"{dataset}.{output_table}",
            project=GCP_PROJECT,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
            ),
        )
    job.result()
