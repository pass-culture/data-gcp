import os
import io
from google.cloud import bigquery
import numpy as np
import os
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
