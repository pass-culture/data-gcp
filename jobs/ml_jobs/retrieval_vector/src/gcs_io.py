import subprocess
import time
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from loguru import logger

from src.constants import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RECOMMENDATION_DATASET,
    GCP_PROJECT_ID,
    MODELS_RESULTS_TABLE_NAME,
)


def download_model(artifact_uri: str) -> None:
    """
    Download model from GCS bucket
    Args:
        artifact_uri (str): GCS bucket path
    """
    command = f"gsutil -m cp -r {artifact_uri} ."
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}: {e.output}")
        raise


def get_items_metadata():
    """
    Retrieve item metadata from BigQuery.

    Fetches comprehensive item information including categories, bookings,
    embeddings, and venue details from the recommendable_item table.

    Returns:
        pd.DataFrame: DataFrame containing item metadata with columns for
            item_id, categories, booking statistics, embeddings, and venue information
    """
    client = bigquery.Client()

    sql = f"""
        SELECT
        item_id,
        topic_id,
        cluster_id,
        category,
        subcategory_id,
        search_group_name,
        is_numerical,
        is_geolocated,
        offer_is_duo,
        offer_type_domain,
        offer_type_label,
        gtl_id,
        gtl_l1,
        gtl_l2,
        gtl_l3,
        gtl_l4,
        booking_number,
        booking_number_last_7_days,
        booking_number_last_14_days,
        booking_number_last_28_days,
        is_underage_recommendable,
        is_sensitive,
        is_restrained,
        offer_creation_date,
        stock_beginning_date,
        stock_price,
        total_offers,
        semantic_emb_mean,
        example_offer_name,
        example_offer_id,
        example_venue_id,
        example_venue_longitude,
        example_venue_latitude,
        booking_trend,
        stock_date_penalty_factor,
        creation_date_penalty_factor,
        booking_release_trend,
        booking_creation_trend,
        ROW_NUMBER() OVER (ORDER BY booking_number DESC) as booking_number_desc,
        ROW_NUMBER() OVER (ORDER BY booking_trend DESC) as booking_trend_desc,
        ROW_NUMBER() OVER (ORDER BY booking_creation_trend DESC) as booking_creation_trend_desc,
        ROW_NUMBER() OVER (ORDER BY booking_release_trend DESC) as booking_release_trend_desc
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_RECOMMENDATION_DATASET}.recommendable_item`
    """
    return client.query(sql).to_dataframe()


def get_users_dummy_metadata():
    """
    Retrieve user IDs from BigQuery for dummy metadata generation.

    Returns:
        pd.DataFrame: DataFrame containing user_id column
    """
    client = bigquery.Client()

    sql = f"""
        SELECT
            user_id
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.global_user`
    """
    return client.query(sql).to_dataframe()


def get_model_from_mlflow(
    experiment_name: str, run_id: str = None, artifact_uri: str = None
):
    """
    Retrieve model artifact URI from MLflow via BigQuery.

    Fetches the artifact URI for a model from BigQuery's MLflow results table,
    either by experiment name (latest run) or by specific run_id.

    Args:
        experiment_name (str): Name of the MLflow experiment
        run_id (str, optional): Specific MLflow run ID. Defaults to None
        artifact_uri (str, optional): Direct artifact URI (returned if valid).
            Defaults to None

    Returns:
        str: GCS artifact URI for the model

    Raises:
        Exception: If model is not found in BigQuery results table
    """
    client = bigquery.Client()

    # get artifact_uri from BQ
    if artifact_uri is None or len(artifact_uri) <= 10:
        if run_id is None or len(run_id) <= 2:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        else:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' AND run_id = '{run_id}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        if len(results_array) == 0:
            raise Exception(
                f"Model {experiment_name} not found into BQ {MODELS_RESULTS_TABLE_NAME}. Failing."
            )
        else:
            artifact_uri = results_array[0]["artifact_uri"]
    return artifact_uri


def save_experiment(experiment_name, model_name, serving_container, run_id):
    """
    Save experiment results to BigQuery.

    Args:
        experiment_name (str): Name of the MLflow experiment
        model_name (str): Name of the model
        serving_container (str): Container image for serving
        run_id (str): MLflow run ID
    """
    log_results = {
        "execution_date": datetime.now().isoformat(),
        "experiment_name": experiment_name,
        "model_name": model_name,
        "model_type": "custom",
        "run_id": run_id,
        "run_start_time": int(time.time() * 1000.0),
        "run_end_time": int(time.time() * 1000.0),
        "artifact_uri": None,
        "serving_container": serving_container,
    }

    client = bigquery.Client()
    table_id = f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}"""

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("execution_date", "STRING"),
            bigquery.SchemaField("experiment_name", "STRING"),
            bigquery.SchemaField("model_name", "STRING"),
            bigquery.SchemaField("model_type", "STRING"),
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("run_start_time", "INTEGER"),
            bigquery.SchemaField("run_end_time", "INTEGER"),
            bigquery.SchemaField("artifact_uri", "STRING"),
            bigquery.SchemaField("serving_container", "STRING"),
        ]
    )
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    df = pd.DataFrame.from_dict([log_results], orient="columns")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
