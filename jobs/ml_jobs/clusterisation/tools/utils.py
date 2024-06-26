import os
import io
from google.cloud import bigquery
from openai import OpenAI
from timeout_decorator import timeout, TimeoutError
from google.cloud import secretmanager
import time
import numpy as np
import json
import hashlib
import base64
import pandas as pd

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


try:
    openai_api_key = get_secret(f"openai_token_{ENV_SHORT_NAME}")
except:
    print("Error, secret not found...")
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    pass

open_client = OpenAI(api_key=openai_api_key)


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


def convert_str_emb_to_float(emb_list):
    float_emb = []
    for str_emb in emb_list:
        emb = json.loads(str_emb)
        float_emb.append(np.array(emb))
    return float_emb


def convert_arr_emb_to_str(emb_list):
    float_emb = []
    for str_emb in emb_list:
        emb = json.dumps(str_emb.tolist())
        float_emb.append(emb)
    return float_emb


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


def call(messages, ttl=5, temperature=0.2, model="gpt-3.5-turbo-1106"):
    @timeout(ttl)
    def _call(messages):
        completion = open_client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=4096,
            n=1,
            stop=None,
            temperature=temperature,
            timeout=ttl,
            response_format={"type": "json_object"},
        )
        return completion.choices[0].message.content

    try:
        return _call(messages)
    except TimeoutError:
        return ""
    except Exception as e:
        print(e)
        time.sleep(60)
        return ""


def call_retry(
    messages,
    test_fn,
    retry=0,
    max_retry=10,
    ttl=20,
    temperature=0.5,
    model="gpt-3.5-turbo-1106",
    default_return={},
):
    while retry <= max_retry:
        try:
            raw = call(messages=messages, ttl=ttl, temperature=temperature, model=model)
            result = json.loads(raw)

        except Exception:
            time.sleep(1)
            result = {}

        if test_fn(result):
            return result
        else:
            return call_retry(
                messages,
                test_fn,
                ttl=ttl,
                retry=retry + 1,
                max_retry=max_retry,
                temperature=temperature,
                model=model,
            )

    return default_return
