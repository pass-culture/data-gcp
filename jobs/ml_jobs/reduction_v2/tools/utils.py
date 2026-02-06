import base64
import hashlib
import io
import json
import os

import pandas as pd
from google.cloud import bigquery, secretmanager

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
CONFIGS_PATH = os.environ.get("CONFIGS_PATH", "configs")
TMP_DATASET = f"tmp_{ENV_SHORT_NAME}"
CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"


def get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def sha1_to_base64(input_string):
    sha1_hash = hashlib.sha1(input_string.encode()).digest()
    base64_encoded = base64.b64encode(sha1_hash).decode()

    return base64_encoded


def load_config_file(config_file_name, job_type):
    with open(
        f"{CONFIGS_PATH}/{job_type}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        return json.load(config_file)


def export_polars_to_bq(data, dataset, output_table):
    client = bigquery.Client()
    with io.BytesIO() as stream:
        data.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=f"{dataset}.{output_table}",
            project=GCP_PROJECT_ID,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
            ),
        )
    job.result()


def load_df(input_table, dataset_id=TMP_DATASET):
    return pd.read_gbq(f"""SELECT * FROM `{dataset_id}.{input_table}`""")
