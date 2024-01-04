import pandas as pd
import numpy as np
import umap
import json
from loguru import logger
import io
from google.cloud import bigquery


def convert_str_emb_to_float(emb_list):
    float_emb = []
    for str_emb in emb_list:
        emb = json.loads(str_emb)
        float_emb.append(np.array(emb))
    return float_emb


def reduce_embedding_dimension(
    data,
    dimension,
):
    float_emb = convert_str_emb_to_float(data)
    logger.info(f"reduction to {dimension} dimensions... size {len(float_emb)}")
    return umap.UMAP(
        n_components=dimension,
        random_state=42,
        transform_seed=42,
        verbose=True,
    ).fit_transform(float_emb)


def export_polars_to_bq(data, project_id, dataset, output_table):
    client = bigquery.Client()
    with io.BytesIO() as stream:
        data.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            project=project_id,
            destination=f"{dataset}.{output_table}",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
            ),
        )
    job.result()
