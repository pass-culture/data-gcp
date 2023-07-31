import pandas as pd
import numpy as np
import umap
import json
from loguru import logger


def convert_str_emb_to_float(emb_list, emb_size=124):
    float_emb = []
    for str_emb in emb_list:
        try:
            emb = json.loads(str_emb)
        except:
            emb = [0] * emb_size
        float_emb.append(np.array(emb))
    return float_emb


def reduce_embedding_dimension(
    data,
    emb_size,
    dimension,
):

    reduced_emb_df = {}
    logger.info(f"Initial emb dimension: {emb_size}")
    float_emb = convert_str_emb_to_float(
        emb_list=data,
        emb_size=emb_size,
    )

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
