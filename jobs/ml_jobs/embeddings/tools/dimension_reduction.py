import pandas as pd
import numpy as np
import umap
import json
from loguru import logger


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
        n_neighbors=10,
        transform_seed=42,
        verbose=False,
    ).fit(float_emb)
    emb_reduced = transformer.embedding_.astype(np.float32)
    return emb_reduced.tolist()
