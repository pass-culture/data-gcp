import concurrent.futures
import io
from urllib.parse import urlencode

import numpy as np
import polars as pl
import requests
from google.cloud import bigquery
from loguru import logger

from utils.constants import (
    APP_CONFIG,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MAX_PROCESS,
    N_RECO_DISPLAY,
)


def get_offline_recos(data):
    """
    Distributes the data across multiple processes to get offline recommendations.

    Args:
        data (pl.DataFrame): Input data containing user and offer information.

    Returns:
        pl.DataFrame: DataFrame containing user IDs and their recommendations.
    """
    subset_length = max(len(data) // MAX_PROCESS, 1)
    batch_number = MAX_PROCESS if subset_length > 1 else 1
    logger.info(
        f"Starting process... with {batch_number} CPUs, subset length: {subset_length}"
    )

    batch_rows = [
        list(chunk) for chunk in np.array_split(data.rows(named=True), batch_number)
    ]
    logger.info(f"And {len(batch_rows)} batches..")

    with concurrent.futures.ThreadPoolExecutor(batch_number) as executor:
        futures = executor.map(_get_recos, batch_rows)

    logger.info("Multiprocessing done")
    return clean_multiprocess_output(futures)


def _get_recos(rows):
    """
    Fetches recommendations for a batch of rows.

    Args:
        rows (list): List of rows containing user and offer information.

    Returns:
        list: List of dictionaries containing user IDs, offer IDs, and recommendations.
    """
    results = []
    for row in rows:
        try:
            reco = similar_offers(
                row["offer_id"], row["venue_longitude"], row["venue_latitude"]
            )[:N_RECO_DISPLAY]
        except Exception as e:
            logger.error(f"Request failed for offer_id {row['offer_id']}: {e}")
            reco = []
        results.append(
            {"user_id": row["user_id"], "offer_id": row["offer_id"], "recos": reco}
        )
    return results


def similar_offers(offer_id, longitude, latitude):
    """
    Fetches similar offers from the API.

    Args:
        offer_id (str): The ID of the offer.
        longitude (float): The longitude of the venue.
        latitude (float): The latitude of the venue.

    Returns:
        list: List of similar offers.
    """
    params_filter = {
        "is_reco_shuffled": False,
    }
    try:
        res = call_API(offer_id, longitude, latitude, params_filter)["results"]
        return res
    except Exception as e:
        logger.error(f"API call failed for offer_id {offer_id}: {e}")
        return []


def call_API(offer_id, longitude, latitude, params_filter):
    """
    Calls the recommendation API.

    Args:
        offer_id (str): The ID of the offer.
        longitude (float): The longitude of the venue.
        latitude (float): The latitude of the venue.
        params_filter (dict): Additional parameters for the API call.

    Returns:
        dict: The API response.
    """
    call = call_builder(offer_id, longitude, latitude)
    return requests.post(call, json=params_filter).json()


def call_builder(offer_id, longitude, latitude):
    """
    Builds the API call URL.

    Args:
        offer_id (str): The ID of the offer.
        longitude (float): The longitude of the venue.
        latitude (float): The latitude of the venue.

    Returns:
        str: The API call URL.
    """
    params = {"token": APP_CONFIG["TOKEN"]}
    if longitude is not None and latitude is not None:
        params.update({"longitude": longitude, "latitude": latitude})
    return f"{APP_CONFIG['URL'][ENV_SHORT_NAME]}/{APP_CONFIG['route']}/{offer_id}?{urlencode(params)}"


def clean_multiprocess_output(futures):
    """
    Cleans and aggregates the output from multiple processes.

    Args:
        futures (list): List of futures containing the results from multiple processes.

    Returns:
        pl.DataFrame: DataFrame containing user IDs and their unique recommendations.
    """
    user_ids = []
    recos = []
    for future in futures:
        for res in future:
            user_ids.append(res["user_id"])
            recos.append(res["recos"])
    return (
        pl.DataFrame({"user_id": user_ids, "recommendations": recos})
        .groupby("user_id")
        .agg(pl.concat_list("recommendations").flatten().unique().drop_nulls())
    )


def export_polars_to_bq(client, data, dataset, output_table):
    """
    Exports a Polars DataFrame to BigQuery.

    Args:
        client (bigquery.Client): The BigQuery client.
        data (pl.DataFrame): The data to export.
        dataset (str): The dataset name.
        output_table (str): The output table name.
    """
    with io.BytesIO() as stream:
        data.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=f"{dataset}.{output_table}",
            project=GCP_PROJECT_ID,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
            ),
        )
    job.result()
