import pandas as pd
import numpy as np
import umap
import json
from loguru import logger
import random


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
    total_size = len(float_emb) // 10

    logger.info(f"reduction to {dimension} dimensions... size {total_size}")
    transformer = umap.UMAP(
        n_components=dimension,
        random_state=42,
        transform_seed=42,
        verbose=True,
    ).fit(random.sample(float_emb, total_size))
    return transformer.transform(float_emb).astype(np.float32)
