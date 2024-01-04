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

    logger.info(f"reduction to {dimension} dimensions...")
    transformer = umap.UMAP(
        n_components=dimension,
        random_state=42,
        transform_seed=42,
        verbose=True,
    ).fit(random.sample(float_emb, len(float_emb) // 10))
    emb_reduced = transformer.transform(float_emb).astype(np.float32)
    return emb_reduced.tolist()
