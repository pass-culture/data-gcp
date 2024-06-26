import time

import polars as pl
import umap
from loguru import logger
from tqdm import tqdm


def reduce_embedding_by_chunk(item_embedding, dim, chunk_size):
    reduction_tmp = []
    nchunks = len(item_embedding) // chunk_size
    logger.info(f"Reducing embedding for {nchunks} chunks...")
    for idx, frame in tqdm(enumerate(item_embedding.iter_slices(n_rows=chunk_size))):
        start = time.time()
        logger.info(f"Reducing embedding for chunk n°{idx}/{nchunks}...")
        reduction_tmp.append(umap_reduction(frame, dim, verbose=False))
        logger.info(f"chunk reduced in: {time.time() - start} seconds.")
    reductions = pl.DataFrame({})
    for reduction in reduction_tmp:
        reductions = pl.concat(
            [
                reductions,
                reduction,
            ],
            how="vertical",
        )

    return reductions


def umap_reduction(data, dim=2, target=None, verbose=True):
    red2 = umap.UMAP(
        n_components=dim,
        random_state=42,
        n_neighbors=10,
        transform_seed=42,
        verbose=verbose,
    )

    logger.info("Fit & Transform data...")
    X2 = red2.fit_transform(data, y=target)
    data_reduced_2d = pl.DataFrame(X2)
    data_reduced_2d.columns = [str(column) for column in data_reduced_2d.columns]
    data_reduced_2d = data_reduced_2d.rename({"column_0": "x", "column_1": "y"})
    data_reduced_2d = data_reduced_2d.select("x", "y")

    return data_reduced_2d
