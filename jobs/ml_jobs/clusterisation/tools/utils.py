import os
import io
from google.cloud import bigquery
import openai

from timeout_decorator import timeout, TimeoutError
import time
import numpy as np
import json

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
CONFIGS_PATH = os.environ.get("CONFIGS_PATH", "configs")


def convert_str_emb_to_float(emb_list, emb_size=5):
    float_emb = []
    for str_emb in emb_list:
        emb = json.loads(str_emb)
        float_emb.append(np.array(emb))
    return float_emb


def export_polars_to_bq(client, data, dataset, output_table):
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


def call(messages, ttl=5, temperature=0.2, model="gpt-3.5-turbo-1106"):
    @timeout(ttl)
    def _call(messages):
        completion = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            max_tokens=4096,
            n=1,
            stop=None,
            temperature=temperature,
            timeout=ttl,
            response_format={"type": "json_object"},
        )
        return completion.choices[0].message["content"]

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

        except Exception as e:
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
